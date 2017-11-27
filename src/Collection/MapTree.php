<?hh // strict
namespace Entropy\Collection;
final class MapTree<Tk as arraykey, Tv> implements Tree<Tk, Tv>, \IteratorAggregate<Pair<Tk, Tv>> {
	public function __construct(
		private Map<Tk, this> $forest = Map{},
		public ?Tv $v = null
		) {
	}
	
	public function __clone(): this {
		return new MapTree($this->forest->map($subtree ==> clone $subtree), $this->v);
	}
	
	public function get_v(): ?Tv {
		return $this->v;
	}
	// public function set_v(Tv $v): this {
	// 	$this->v = $v;
	// 	return $this->v;
	// }
	
	public function subtree_at(Tk $k): this {
		return $this->forest->at($k);
	}
	
	public function get_subtree(Tk $k): ?this {
		return $this->forest->get($k);
	}
	
	public function get_forest(): Map<Tk, this> {
		return $this->forest;
	}
	
	public function set_subtree(Tk $k, this $incoming): void {
		$this->forest->set($k, $incoming);
	}
	
	public function getIterator(): Iterator<Pair<Tk, Tv>> {
		foreach($this->forest as $subtree_k => $subtree) {
			$subtree_v = $subtree->v;
			if(!is_null($subtree_v))
				yield Pair{ $subtree_k, $subtree_v };
			
			foreach($subtree as $v)
				yield $v;
		}
	}
	
	public static function fromArray(array<mixed> $incoming): MapTree<arraykey, mixed> {
		$ret = Map{};
		foreach($incoming as $k => $v) {
			if(is_array($v))
				$ret->set($k, self::fromArray($v));
			else
				$ret->set($k, new MapTree(Map{}, $v));
		}
		return new MapTree($ret, null);
	}
	
	public function to_dict(): dict<Tk, mixed> {
		// lose values of nodes, and preserve only for leaves
		$ret = dict[];
		foreach($this->forest as $k => $subtree) {
			if($subtree->get_forest()->count() === 0)
				$ret[$k] = $subtree->get_v();
			else
				$ret[$k] = $subtree->to_dict();
		}
		return $ret;
	}
}