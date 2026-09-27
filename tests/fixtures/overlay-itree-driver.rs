#[path = "../../src/overlay/itree.rs"]
mod itree;
use itree::{Node, NodeRef, Traversal, Tree};
use std::io::{self, BufRead, Write};
fn main() {
    let mut tree = Tree::default();
    let mut nodes: Vec<Option<Box<Node>>> = (0..256).map(|_| None).collect();
    let mut out = io::BufWriter::new(io::stdout().lock());
    for (step, line) in io::stdin().lock().lines().enumerate() {
        let line = line.unwrap();
        let mut parts = line.split_whitespace();
        let op = parts.next().unwrap();
        let args: Vec<isize> = parts.map(|p| p.parse().unwrap()).collect();
        let node = |id: isize| unsafe {
            NodeRef::from_raw(&**nodes[id as usize].as_ref().unwrap() as *const Node as *mut Node)
        };
        match op {
            "I" => {
                let id = args[0] as usize;
                assert!(nodes[id].is_none());
                let mut owned = Box::new(Node::new(id, args[3] != 0, args[4] != 0));
                owned.set_data(id);
                owned.set_detached_bounds(-1, -1);
                let reference = unsafe { NodeRef::from_raw(&mut *owned) };
                assert_eq!(reference.as_ptr(), &*owned as *const Node as *mut Node);
                tree.insert(reference, args[1], args[2]);
                nodes[id] = Some(owned);
            }
            "R" => {
                tree.remove(node(args[0]));
                nodes[args[0] as usize] = None;
            }
            "M" => tree.set_region(node(args[0]), args[1], args[2]),
            "+" => tree.insert_gap(args[0], args[1], args[2] != 0),
            "-" => tree.delete_gap(args[0], args[1]),
            "Q" => {
                let order = [
                    Traversal::Ascending,
                    Traversal::Descending,
                    Traversal::PreOrder,
                    Traversal::PostOrder,
                ][args[2] as usize];
                let mut iter = tree.iter(args[0], args[1], order);
                write!(out, "Q{}", step + 1).unwrap();
                let mut first = true;
                while let Some(n) = iter.next() {
                    let (begin, end) = n.bounds();
                    write!(out, " {}:{}:{}", n.data(), begin, end).unwrap();
                    if first && args[3] >= 0 {
                        iter.narrow(args[3], args[4]);
                    }
                    first = false;
                }
                writeln!(out).unwrap();
            }
            "S" => {
                let mut count = 0;
                tree.visit_data(&mut |_| count += 1);
                assert_eq!(tree.len(), count);
                write!(out, "S{} {}", step + 1, tree.len()).unwrap();
                for (id, n) in nodes.iter().enumerate() {
                    if n.is_some() {
                        let (b, e) = tree.bounds(node(id as isize));
                        write!(out, " {id}:{b}:{e}").unwrap();
                    }
                }
                writeln!(out).unwrap();
            }
            _ => panic!("unknown operation"),
        }
    }
    // Detach before dropping owned nodes, matching buffer destruction.
    for node in tree.iter(isize::MIN, isize::MAX, Traversal::PostOrder) {
        node.clear_links();
    }
    tree.clear();
    assert!(tree.is_empty());
}
