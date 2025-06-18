use petgraph::Graph;
use petgraph::algo::page_rank; // 需要添加petgraph-algo特性

fn main() {
    println!("Hello, world!");
    let mut graph = Graph::<(), ()>::new();
    let a = graph.add_node(()); // 添加节点
    let b = graph.add_node(());
    let c = graph.add_node(());
    
    graph.add_edge(a, b, ()); // 添加边
    graph.add_edge(b, c, ());
    graph.add_edge(c, a, ());
    
    let damping_factor = 0.85;
    let iterations = 20;
    let pagerank_scores = petgraph::algo::page_rank(&graph, damping_factor, iterations);
    
    println!("PageRank scores: {:?}", pagerank_scores);
}