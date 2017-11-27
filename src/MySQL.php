<?hh // strict
namespace Entropy;
use PHPSQLParser\PHPSQLParser;
use PHPSQLParser\exceptions\UnsupportedFeatureException; // not so fond of this.
use Entropy\Collection\MapTree;

use namespace HH\Lib\{C, Dict, Vec};

newtype Expression = shape("columns" => Vector<string>, "expr_tree" => dict<string, mixed>); // expr_tree can't be flattened until the end because the builder might not appreciate the base_expr of a replaced node not matching the rest of the structure

class MySQL {
	public function __construct(
		private \AsyncMysqlConnection $conn
	) {}
	
	public async function query(string $sql): Awaitable<\AsyncMysqlResult> {
		$parser = new PHPSQLParser($sql);
		$parsed = MapTree::fromArray($parser->parsed);
	}
	
	private static function lift_joined_columns(dict<arraykey, mixed> $subquery): dict<arraykey, mixed> {
		if(C\contains($subquery, 'FROM')) {
			// no tables -> no change
			$tables = $subquery['FROM'];
			invariant(is_array($tables), 'Structural contract with PHPSQLParser');
			$main_table = $tables[0];
			invariant(is_array($main_table), 'Structural contract with PHPSQLParser: of "FROM" exists, it must contain at least one table.');
			
			
			$aliased_expressions = self::dealias_subquery_exprs($subquery);
			foreach(Vec\slice($tables, 1) as $join) {
				$join_table_name = '';
				if($join['alias'] !== false)
					$join_table_name = $join['alias']['name'];
				else
					// MUST be table (since all subqueries have aliases)
					$join_table_name = $join['table'];
				
				if($join['ref_type'] === 'USING') {
					foreach($join['ref_clause'] as $k => $ref) {
						// USING a bit wonky rn; e.g. with `SELECT * FROM a JOIN b USING (c, d)` it doesn't give any columns in the column list
						// DEFER: look into it later
						
						$ref_name = $ref['base_expr'];
						
						$column_expr = dict[
							"expr_type" => "colref",
							"alias" => false,
							"base_expr" => "$join_table_name.$ref_name",
							"no_quotes" => dict[
								"delim" => ".",
								"parts" => vec[
									$join_table_name,
									$ref_name
								]
							]
						];
						
						if($join['expr_type'] === 'subquery') {
							foreach($join['subtree']['SELECT'] as $col) {
								// **WAIT**. I don't want to dealias for this, right? I want to just use the straight columns, with an extra check for non-derivedness
								$col_name = $col['alias'] !== false ? $col['alias']['name'] : $col['base_expr'];
								if($col_name === $ref_name) {
									$maybe_aliased_expr = $aliased_expressions->get($join_table_name)?->get($col_name);
									if(!is_null($maybe_aliased_expr)) {
										if(self::is_plain_colref($maybe_aliased_expr['expr_tree'])) {
											/* HH_IGNORE_ERROR[4006] Can't refine type to nested dict for write due to copy-on-write mechanics */
											$subquery['SELECT'][] = $column_expr;
											break;
										}
										else {
											throw new \UnexpectedValueException('Derived join columns cannot be analyzed.');
										}
									}
									elseif($col['expr_type'] === 'subquery')
										/* HH_IGNORE_ERROR[4006] Can't refine type to nested dict for write due to copy-on-write mechanics */
										$subquery['SELECT'][] = $column_expr;
								}
							}
						}
						else {
							/* HH_IGNORE_ERROR[4006] Can't refine type to nested dict for write due to copy-on-write mechanics */
							$subquery['SELECT'][] = $column_expr;
						}
					}
				}
				elseif($join['ref_type'] === 'NATURAL') {
					throw new UnsupportedFeatureException('NATURAL JOIN');
				}
				else {
					foreach($join['ref_clause'] as $joiner) {
						
					}
				}
			}
		}
		return $subquery;
	}
	
	private static function exprs_from_subquery(dict<arraykey, mixed> $subquery): Map<string, Expression> {
		$aliased_expressions = self::dealias_subquery_exprs($subquery);
		$resolved_columns = Map{};
		// resolve expressions and column references and grow the basket of returned columns
		invariant(C\contains($subquery, 'SELECT'), 'Only SELECT statements allowed');
		$statement = $subquery['SELECT'];
		invariant(is_array($statement), 'Structural contract with PHPSQLParser');
		foreach($statement as $column_expr) {
			$column_name = '';
			if($column_expr['alias'] !== false) {
				$column_name = $column_expr['alias']['name'];
			}
			else {
				if($column_expr['expr_type'] === 'colref') {
					$parser = new \ParseSQLIdentifier\Parser();
					$column_name = $parser->parse($column_expr->subtree_at('base_expr'))[2]; // extract column name
				}
				else {
					// 1. throw new \UnexpectedValueException('Expressions without aliases cannot be analyzed.'); // don't know if we need to be this harsh; technically the superquery could access an expression with backticks like "subquery.`COUNT(*)`"
					// 2. $column_name = $column_expr['base_expr'];
					// 3. continue;
					
					// ^ which one of the three will it be? ^
					continue;
				}
			}
			
			// BFS over potential expression tree
			$wrapped_column_expr = Vector{ $column_expr }; // inner wrapper for mutability
			$front = Vector{ $wrapped_column_expr };
			$column_dependencies = Vector{};
			while(!$front->isEmpty()) {
				$prev_front = $front;
				$front = Vector{};
				foreach($prev_front as $subtree) {
					for($subexpr_key = 0; $subexpr_key < $subtree->count(); $subexpr_key++) {
						$subexpr = $subtree[$subexpr_key];
						$expr_type = $subexpr['expr_type'];
						invariant(is_string($expr_type), 'Structural contract with PHPSQLParser: all expressions have expr_type');
						switch($expr_type) {
							case 'expression': // FALLTHROUGH
							case 'function': // FALLTHROUGH
							case 'aggregate_function':
								$front->add($subexpr['sub_tree']);
								break;
							case 'colref':
								$lexed_name = $subexpr['no_quotes']['parts'];
								if(count($lexed_name) === 3) // database identifier
									throw new UnsupportedFeatureException('Column refs with database identifiers');
								elseif(count($lexed_name) === 1) // raw column reference, origin table ambiguous (unless there is exactly one)
									throw new UnsupportedFeatureException('Column refs without table identifiers');
								else {
									$sub_table_name = $lexed_name[0];
									$sub_column_name = $lexed_name[1];
									// invariant(is_string($sub_table_name) && is_string($sub_column_name), 'Structural contract with PHPSQLParser + user constraint: colrefs must have exactly two identifiers: table and column names');
									$dealiased_expr = $aliased_expressions->get($sub_table_name)?->get($sub_column_name);
									if(!is_null($dealiased_expr)) {
										$subtree[$subexpr_key] = $dealiased_expr['expr_tree']; // replace column reference in expression with the whole aliased subexpression
										$column_dependencies->addAll($dealiased_expr['columns']); // log that this expression depends on all columns from the dealiased subexpression
									}
									else
										$column_dependencies->add($subexpr->subtree_at('base_expr')); // log that this expression depends on this direct column reference
								}
								break;
						}
					}
				}
			}
			$resolved_columns->set($column_name, shape('columns' => $column_dependencies, 'expr_tree' => $wrapped_column_expr[0]));
		}
		return $resolved_columns;
		// 
	}
	
	<<__Memoize>>
	private static function dealias_subquery_exprs(dict<arraykey, mixed> $subquery): \ConstMap<string, \ConstMap<string, Expression>> {
		// dealias expressions from joined subqueries
		$aliased_expressions = Map{};
		if(C\contains($subquery, 'FROM')) {
			// note: column expressions can't refer to each other's aliases (forward reference in item list not supported, error 1247), so alias names are purely for the immediate superquery
			$tables = $subquery['FROM'];
			invariant(is_array($tables), 'Structural contract with PHPSQLParser - FROM clause if exists is array of table expressions');
			foreach($tables as $join) {
				// invariant(!is_null($joining_table), 'Structure contract with PHPSQLParser - all joins have a table name');
				
				switch($join['expr_type']) {
					case 'table':
						// no-op for now
						break;
					case 'subquery':
						// all proper subqueries must have aliases
						$aliased_expressions->set($join['alias']['name'], self::exprs_from_subquery($join['sub_tree']));
						break;
					default:
						throw new \UnexpectedValueException('Unexpected join expression: table or subquery expected.'); // I don't discount this possibility in some weird-ass edge case
						break;
				}
			}
			return $aliased_expressions;
		}
		else {
			// the root query has only uncorrelated subqueries and pure expressions (hopefully no randomness)
			return Map{};
		}
	}
	
	private static function is_plain_colref(dict<string, mixed> $expr): bool {
		return $expr['expr_type'] === 'colref';
	}
}