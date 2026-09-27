//! GNU Emacs itree.c's augmented red-black tree and deferred edit offsets.
//!
//! Derived from GNU Emacs, Copyright (C) 2017-2025 Free Software Foundation,
//! Inc. Distributed under the GNU General Public License, version 3 or later.
//!
//! Nodes belong to overlays, not to the index. A linked node must remain at
//! its allocated address until removed. All mutable node fields use Cell:
//! offset validation and generated-code field access must not create aliased
//! mutable Rust references. Tree mutation requires an exclusive tree borrow.

use std::cell::Cell;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::NonNull;

#[repr(C)]
pub(super) struct Node {
    parent: Cell<Option<NodeRef>>,
    left: Cell<Option<NodeRef>>,
    right: Cell<Option<NodeRef>>,
    begin: Cell<isize>,
    end: Cell<isize>,
    limit: Cell<isize>,
    offset: Cell<isize>,
    otick: Cell<u64>,
    data: Cell<usize>,
    flags: Cell<u8>,
    padding: [u8; 7],
}

const RED: u8 = 1;
const REAR_ADVANCE: u8 = 2;
const FRONT_ADVANCE: u8 = 4;

const _: () = {
    assert!(std::mem::size_of::<Node>() == 80);
    assert!(std::mem::offset_of!(Node, begin) == 24);
    assert!(std::mem::offset_of!(Node, end) == 32);
    assert!(std::mem::offset_of!(Node, data) == 64);
    assert!(std::mem::offset_of!(Node, flags) == 72);
};

impl Node {
    pub(super) fn new(data: usize, front_advance: bool, rear_advance: bool) -> Self {
        Self {
            parent: Cell::new(None),
            left: Cell::new(None),
            right: Cell::new(None),
            begin: Cell::new(-1),
            end: Cell::new(-1),
            limit: Cell::new(0),
            offset: Cell::new(0),
            otick: Cell::new(0),
            data: Cell::new(data),
            flags: Cell::new(
                (u8::from(front_advance) * FRONT_ADVANCE) | (u8::from(rear_advance) * REAR_ADVANCE),
            ),
            padding: [0; 7],
        }
    }

    pub(super) fn data(&self) -> usize {
        self.data.get()
    }

    pub(super) fn set_data(&self, data: usize) {
        self.data.set(data);
    }

    pub(super) fn front_advance(&self) -> bool {
        self.flags.get() & FRONT_ADVANCE != 0
    }

    pub(super) fn rear_advance(&self) -> bool {
        self.flags.get() & REAR_ADVANCE != 0
    }

    fn red(&self) -> bool {
        self.flags.get() & RED != 0
    }

    fn set_red(&self, red: bool) {
        self.flags.set((self.flags.get() & !RED) | u8::from(red));
    }

    pub(super) fn bounds(&self) -> (isize, isize) {
        (self.begin.get(), self.end.get())
    }

    pub(super) fn set_detached_bounds(&self, begin: isize, end: isize) {
        debug_assert!(self.parent.get().is_none());
        debug_assert!(self.left.get().is_none() && self.right.get().is_none());
        self.begin.set(begin);
        self.end.set(end);
    }

    pub(super) fn clear_links(&self) {
        self.parent.set(None);
        self.left.set(None);
        self.right.set(None);
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct NodeRef(NonNull<Node>);

impl NodeRef {
    pub(super) fn as_ptr(self) -> *mut Node {
        self.0.as_ptr()
    }

    /// # Safety
    /// The node is initialized, pinned, and remains allocated for every use
    /// of this reference, including references retained in a tree or iterator.
    pub(super) unsafe fn from_raw(node: *mut Node) -> Self {
        Self(NonNull::new(node).expect("an interval node is allocated"))
    }
}

impl Deref for NodeRef {
    type Target = Node;

    fn deref(&self) -> &Node {
        // SAFETY: the constructor's lifetime and pinning contract. Mutations
        // of the node always go through its interior-mutable fields.
        unsafe { self.0.as_ref() }
    }
}

fn is_red(node: Option<NodeRef>) -> bool {
    node.is_some_and(|node| node.red())
}

fn new_limit(node: NodeRef) -> isize {
    node.end.get().max(
        node.left
            .get()
            .map_or(isize::MIN, |left| left.limit.get() + left.offset.get())
            .max(
                node.right
                    .get()
                    .map_or(isize::MIN, |right| right.limit.get() + right.offset.get()),
            ),
    )
}

fn update_limit(node: NodeRef) {
    node.limit.set(new_limit(node));
}

fn propagate_limit(mut current: Option<NodeRef>) {
    while let Some(node) = current {
        let limit = new_limit(node);
        if limit == node.limit.get() {
            break;
        }
        node.limit.set(limit);
        current = node.parent.get();
    }
}

fn inherit_offset(otick: u64, node: NodeRef) {
    if node.otick.get() == otick {
        debug_assert_eq!(node.offset.get(), 0);
        return;
    }
    let offset = node.offset.replace(0);
    if offset != 0 {
        node.begin.set(node.begin.get() + offset);
        node.end.set(node.end.get() + offset);
        node.limit.set(node.limit.get() + offset);
        for child in [node.left.get(), node.right.get()].into_iter().flatten() {
            child.offset.set(child.offset.get() + offset);
        }
    }
    if node
        .parent
        .get()
        .is_none_or(|parent| parent.otick.get() == otick)
    {
        node.otick.set(otick);
    }
}

#[repr(C)]
pub(super) struct Tree {
    root: Cell<Option<NodeRef>>,
    otick: Cell<u64>,
    size: Cell<i64>,
}

impl Default for Tree {
    fn default() -> Self {
        Self {
            root: Cell::new(None),
            otick: Cell::new(1),
            size: Cell::new(0),
        }
    }
}

impl Tree {
    pub(super) fn len(&self) -> usize {
        self.size.get() as usize
    }

    pub(super) fn is_empty(&self) -> bool {
        self.root.get().is_none()
    }

    pub(super) fn clear(&mut self) {
        self.root.set(None);
        self.otick.set(1);
        self.size.set(0);
    }

    pub(super) fn validate(&self, node: NodeRef) {
        if node.otick.get() == self.otick.get() {
            return;
        }
        if Some(node) != self.root.get() {
            self.validate(node.parent.get().expect("a linked non-root has a parent"));
        }
        inherit_offset(self.otick.get(), node);
    }

    pub(super) fn bounds(&self, node: NodeRef) -> (isize, isize) {
        self.validate(node);
        node.bounds()
    }

    fn rotate_left(&mut self, node: NodeRef) {
        let right = node.right.get().expect("left rotation has a right child");
        inherit_offset(self.otick.get(), node);
        inherit_offset(self.otick.get(), right);
        node.right.set(right.left.get());
        if let Some(left) = right.left.get() {
            left.parent.set(Some(node));
        }
        right.parent.set(node.parent.get());
        if let Some(parent) = node.parent.get() {
            if parent.left.get() == Some(node) {
                parent.left.set(Some(right));
            } else {
                parent.right.set(Some(right));
            }
        } else {
            self.root.set(Some(right));
        }
        right.left.set(Some(node));
        node.parent.set(Some(right));
        update_limit(node);
        update_limit(right);
    }

    fn rotate_right(&mut self, node: NodeRef) {
        let left = node.left.get().expect("right rotation has a left child");
        inherit_offset(self.otick.get(), node);
        inherit_offset(self.otick.get(), left);
        node.left.set(left.right.get());
        if let Some(right) = left.right.get() {
            right.parent.set(Some(node));
        }
        left.parent.set(node.parent.get());
        if let Some(parent) = node.parent.get() {
            if parent.right.get() == Some(node) {
                parent.right.set(Some(left));
            } else {
                parent.left.set(Some(left));
            }
        } else {
            self.root.set(Some(left));
        }
        left.right.set(Some(node));
        node.parent.set(Some(left));
        update_limit(left);
        update_limit(node);
    }

    fn insert_fix(&mut self, mut node: NodeRef) {
        while is_red(node.parent.get()) {
            let parent = node.parent.get().expect("a red parent exists");
            let grandparent = parent.parent.get().expect("the root is black");
            if grandparent.left.get() == Some(parent) {
                let uncle = grandparent.right.get();
                if is_red(uncle) {
                    parent.set_red(false);
                    uncle.expect("red uncle exists").set_red(false);
                    grandparent.set_red(true);
                    node = grandparent;
                } else {
                    if parent.right.get() == Some(node) {
                        node = parent;
                        self.rotate_left(node);
                    }
                    let parent = node.parent.get().expect("parent after rotation");
                    let grandparent = parent.parent.get().expect("grandparent after rotation");
                    parent.set_red(false);
                    grandparent.set_red(true);
                    self.rotate_right(grandparent);
                }
            } else {
                let uncle = grandparent.left.get();
                if is_red(uncle) {
                    parent.set_red(false);
                    uncle.expect("red uncle exists").set_red(false);
                    grandparent.set_red(true);
                    node = grandparent;
                } else {
                    if parent.left.get() == Some(node) {
                        node = parent;
                        self.rotate_right(node);
                    }
                    let parent = node.parent.get().expect("parent after rotation");
                    let grandparent = parent.parent.get().expect("grandparent after rotation");
                    parent.set_red(false);
                    grandparent.set_red(true);
                    self.rotate_left(grandparent);
                }
            }
        }
        self.root
            .get()
            .expect("inserted tree is nonempty")
            .set_red(false);
    }

    fn insert_node(&mut self, node: NodeRef) {
        debug_assert!(node.parent.get().is_none());
        debug_assert!(node.left.get().is_none() && node.right.get().is_none());
        debug_assert_eq!(node.otick.get(), self.otick.get());
        let mut parent = None;
        let mut child = self.root.get();
        while let Some(current) = child {
            inherit_offset(self.otick.get(), current);
            parent = Some(current);
            current.limit.set(current.limit.get().max(node.end.get()));
            child = if node.begin.get() <= current.begin.get() {
                current.left.get()
            } else {
                current.right.get()
            };
        }
        if let Some(parent) = parent {
            if node.begin.get() <= parent.begin.get() {
                parent.left.set(Some(node));
            } else {
                parent.right.set(Some(node));
            }
        } else {
            self.root.set(Some(node));
        }
        node.parent.set(parent);
        node.offset.set(0);
        node.limit.set(node.end.get());
        self.size.set(self.size.get() + 1);
        if self.root.get() == Some(node) {
            node.set_red(false);
        } else {
            node.set_red(true);
            self.insert_fix(node);
        }
    }

    /// The caller owns NODE and ensures it is not already linked in a tree.
    pub(super) fn insert(&mut self, node: NodeRef, begin: isize, end: isize) {
        assert!(begin <= end);
        node.begin.set(begin);
        node.end.set(end);
        node.otick.set(self.otick.get());
        self.insert_node(node);
    }

    pub(super) fn set_region(&mut self, node: NodeRef, begin: isize, end: isize) {
        self.validate(node);
        if begin != node.begin.get() {
            self.remove(node);
            let begin = begin.min(isize::MAX - 1);
            node.begin.set(begin);
            node.end.set(end.max(begin));
            self.insert_node(node);
        } else if end != node.end.get() {
            node.end.set(end.max(begin));
            propagate_limit(Some(node));
        }
    }

    fn replace_child(&mut self, source: Option<NodeRef>, dest: NodeRef) {
        let parent = dest.parent.get();
        if let Some(parent) = parent {
            if parent.left.get() == Some(dest) {
                parent.left.set(source);
            } else {
                parent.right.set(source);
            }
        } else {
            self.root.set(source);
        }
        if let Some(source) = source {
            source.parent.set(parent);
        }
    }

    fn transplant(&mut self, source: NodeRef, dest: NodeRef) {
        self.replace_child(Some(source), dest);
        source.left.set(dest.left.get());
        if let Some(left) = source.left.get() {
            left.parent.set(Some(source));
        }
        source.right.set(dest.right.get());
        if let Some(right) = source.right.get() {
            right.parent.set(Some(source));
        }
        source.set_red(dest.red());
    }

    fn remove_fix(&mut self, mut node: Option<NodeRef>, mut parent: Option<NodeRef>) {
        while let Some(current) = parent {
            if is_red(node) {
                break;
            }
            if current.left.get() == node {
                let mut other = current.right.get().expect("black-height sibling exists");
                if other.red() {
                    other.set_red(false);
                    current.set_red(true);
                    self.rotate_left(current);
                    other = current.right.get().expect("sibling after rotation");
                }
                if !is_red(other.left.get()) && !is_red(other.right.get()) {
                    other.set_red(true);
                    node = Some(current);
                    parent = current.parent.get();
                } else {
                    if !is_red(other.right.get()) {
                        other.left.get().expect("red left child").set_red(false);
                        other.set_red(true);
                        self.rotate_right(other);
                        other = current.right.get().expect("sibling after rotation");
                    }
                    other.set_red(current.red());
                    current.set_red(false);
                    other.right.get().expect("red right child").set_red(false);
                    self.rotate_left(current);
                    node = self.root.get();
                    parent = None;
                }
            } else {
                let mut other = current.left.get().expect("black-height sibling exists");
                if other.red() {
                    other.set_red(false);
                    current.set_red(true);
                    self.rotate_right(current);
                    other = current.left.get().expect("sibling after rotation");
                }
                if !is_red(other.right.get()) && !is_red(other.left.get()) {
                    other.set_red(true);
                    node = Some(current);
                    parent = current.parent.get();
                } else {
                    if !is_red(other.left.get()) {
                        other.right.get().expect("red right child").set_red(false);
                        other.set_red(true);
                        self.rotate_left(other);
                        other = current.left.get().expect("sibling after rotation");
                    }
                    other.set_red(current.red());
                    current.set_red(false);
                    other.left.get().expect("red left child").set_red(false);
                    self.rotate_right(current);
                    node = self.root.get();
                    parent = None;
                }
            }
        }
        if let Some(node) = node {
            node.set_red(false);
        }
    }

    /// NODE belongs to this tree. Validate its position before unlinking,
    /// as buffer.c does when it reports the region an overlay is leaving.
    pub(super) fn remove(&mut self, node: NodeRef) {
        self.validate(node);
        let splice = if node.left.get().is_none() || node.right.get().is_none() {
            node
        } else {
            let mut next = node.right.get().expect("two children");
            loop {
                inherit_offset(self.otick.get(), next);
                match next.left.get() {
                    Some(left) => next = left,
                    None => break next,
                }
            }
        };
        let subtree = splice.left.get().or_else(|| splice.right.get());
        let subtree_parent = if splice.parent.get() != Some(node) {
            splice.parent.get()
        } else {
            Some(splice)
        };
        self.replace_child(subtree, splice);
        let removed_black = !splice.red();
        if splice != node {
            self.transplant(splice, node);
            propagate_limit(subtree_parent);
            if subtree_parent != Some(splice) {
                update_limit(splice);
            }
        }
        propagate_limit(splice.parent.get());
        self.size.set(self.size.get() - 1);
        if removed_black {
            self.remove_fix(subtree, subtree_parent);
        }
        node.set_red(false);
        node.clear_links();
        node.limit.set(0);
        debug_assert_eq!(node.otick.get(), self.otick.get());
        debug_assert_eq!(node.offset.get(), 0);
    }

    fn stack_capacity(&self) -> usize {
        // itree_max_height: twice log2(size + 1), rounded to nearest.
        (2.0 * (self.size.get() as f64 + 1.0).log2() + 0.5) as usize + 1
    }

    pub(super) fn insert_gap(&mut self, pos: isize, length: isize, before_markers: bool) {
        if length <= 0 || self.is_empty() {
            return;
        }
        let mut saved = Vec::new();
        if !before_markers {
            for node in self.iter(pos, pos + 1, Traversal::PreOrder) {
                if node.begin.get() == pos
                    && node.front_advance()
                    && (node.begin.get() != node.end.get() || node.rear_advance())
                {
                    saved.push(node);
                }
            }
        }
        for node in &saved {
            self.remove(*node);
        }
        if let Some(root) = self.root.get() {
            let mut stack = Vec::with_capacity(self.stack_capacity());
            stack.push(root);
            while let Some(node) = stack.pop() {
                inherit_offset(self.otick.get(), node);
                if pos > node.limit.get() {
                    continue;
                }
                if let Some(right) = node.right.get() {
                    if node.begin.get() > pos {
                        right.offset.set(right.offset.get() + length);
                        self.otick.set(self.otick.get().wrapping_add(1));
                    } else {
                        stack.push(right);
                    }
                }
                if let Some(left) = node.left.get() {
                    stack.push(left);
                }
                if node.begin.get() > pos || (before_markers && node.begin.get() == pos) {
                    node.begin.set(node.begin.get() + length);
                }
                if node.end.get() > pos
                    || (node.end.get() == pos && (before_markers || node.rear_advance()))
                {
                    node.end.set(node.end.get() + length);
                    propagate_limit(Some(node));
                }
            }
        }
        while let Some(node) = saved.pop() {
            node.begin.set(node.begin.get() + length);
            node.end.set(node.end.get() + length);
            node.otick.set(self.otick.get());
            self.insert_node(node);
        }
    }

    pub(super) fn delete_gap(&mut self, pos: isize, length: isize) {
        let Some(root) = self.root.get().filter(|_| length > 0) else {
            return;
        };
        let mut stack = Vec::with_capacity(self.stack_capacity());
        stack.push(root);
        while let Some(node) = stack.pop() {
            inherit_offset(self.otick.get(), node);
            if pos > node.limit.get() {
                continue;
            }
            if let Some(right) = node.right.get() {
                if node.begin.get() > pos + length {
                    right.offset.set(right.offset.get() - length);
                    self.otick.set(self.otick.get().wrapping_add(1));
                } else {
                    stack.push(right);
                }
            }
            if let Some(left) = node.left.get() {
                stack.push(left);
            }
            if pos < node.begin.get() {
                node.begin.set(pos.max(node.begin.get() - length));
            }
            if pos < node.end.get() {
                node.end.set(pos.max(node.end.get() - length));
                propagate_limit(Some(node));
            }
        }
    }

    pub(super) fn iter(&self, begin: isize, end: isize, order: Traversal) -> Iter<'_> {
        let mut iter = Iter {
            next: self.root.get(),
            begin,
            end,
            otick: self.otick.get(),
            order,
            tree: PhantomData,
        };
        if let Some(mut node) = iter.next {
            inherit_offset(iter.otick, node);
            match order {
                Traversal::Ascending => {
                    while let Some(left) = iter.left_in_range(node) {
                        node = left;
                    }
                    if node.begin.get() > end {
                        iter.next = None;
                        return iter;
                    }
                }
                Traversal::Descending => {
                    if node.limit.get() < begin {
                        iter.next = None;
                        return iter;
                    }
                    while let Some(right) = iter.right_in_range(node) {
                        node = right;
                    }
                }
                Traversal::PreOrder => {}
                Traversal::PostOrder => {
                    while let Some(child) = iter
                        .left_in_range(node)
                        .or_else(|| iter.right_in_range(node))
                    {
                        node = child;
                    }
                }
            }
            iter.next = Some(node);
        }
        iter
    }

    /// alloc.c:mark_overlays reads links and payloads without forcing the
    /// deferred positional offsets. No auxiliary index or node registry.
    pub(super) fn visit_data(&self, visit: &mut impl FnMut(usize)) {
        fn visit_node(node: Option<NodeRef>, visit: &mut impl FnMut(usize)) {
            if let Some(node) = node {
                visit(node.data());
                visit_node(node.left.get(), visit);
                visit_node(node.right.get(), visit);
            }
        }
        visit_node(self.root.get(), visit);
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Traversal {
    Ascending,
    Descending,
    PreOrder,
    PostOrder,
}

pub(super) struct Iter<'a> {
    next: Option<NodeRef>,
    begin: isize,
    end: isize,
    otick: u64,
    order: Traversal,
    tree: PhantomData<&'a Tree>,
}

impl Iter<'_> {
    pub(super) fn narrow(&mut self, begin: isize, end: isize) {
        debug_assert!(begin >= self.begin && end <= self.end);
        self.begin = self.begin.max(begin);
        self.end = self.end.min(end);
    }

    fn left_in_range(&self, node: NodeRef) -> Option<NodeRef> {
        node.left.get().filter(|left| {
            inherit_offset(self.otick, *left);
            self.begin <= left.limit.get()
        })
    }

    fn right_in_range(&self, node: NodeRef) -> Option<NodeRef> {
        node.right.get().filter(|right| {
            if node.begin.get() > self.end {
                return false;
            }
            inherit_offset(self.otick, *right);
            true
        })
    }

    fn next_in_subtree(&self, mut node: NodeRef) -> Option<NodeRef> {
        match self.order {
            Traversal::Ascending => {
                if let Some(right) = node.right.get() {
                    node = right;
                    inherit_offset(self.otick, node);
                    while let Some(left) = self.left_in_range(node) {
                        node = left;
                    }
                } else {
                    loop {
                        let parent = node.parent.get()?;
                        if parent.right.get() != Some(node) {
                            node = parent;
                            break;
                        }
                        node = parent;
                    }
                }
                (node.begin.get() <= self.end).then_some(node)
            }
            Traversal::Descending => {
                if let Some(left) = self.left_in_range(node) {
                    node = left;
                    while let Some(right) = self.right_in_range(node) {
                        node = right;
                    }
                } else {
                    loop {
                        let parent = node.parent.get()?;
                        if parent.left.get() != Some(node) {
                            node = parent;
                            break;
                        }
                        node = parent;
                    }
                }
                Some(node)
            }
            Traversal::PreOrder => {
                if let Some(child) = self
                    .left_in_range(node)
                    .or_else(|| self.right_in_range(node))
                {
                    return Some(child);
                }
                while let Some(parent) = node.parent.get() {
                    if parent.right.get() != Some(node)
                        && let Some(right) = self.right_in_range(parent)
                    {
                        return Some(right);
                    }
                    node = parent;
                }
                None
            }
            Traversal::PostOrder => {
                let parent = node.parent.get()?;
                if parent.right.get() == Some(node) {
                    return Some(parent);
                }
                let Some(right) = self.right_in_range(parent) else {
                    return Some(parent);
                };
                node = right;
                while let Some(child) = self
                    .left_in_range(node)
                    .or_else(|| self.right_in_range(node))
                {
                    node = child;
                }
                Some(node)
            }
        }
    }
}

impl Iterator for Iter<'_> {
    type Item = NodeRef;

    fn next(&mut self) -> Option<NodeRef> {
        while let Some(node) = self.next {
            self.next = self.next_in_subtree(node);
            if (self.begin < node.end.get() && node.begin.get() < self.end)
                || (node.begin.get() == node.end.get() && self.begin == node.begin.get())
            {
                return Some(node);
            }
        }
        None
    }
}
