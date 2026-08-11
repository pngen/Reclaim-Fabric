//! Lineage and dependency safety.
//!
//! Lineage is a typed DAG over objects:
//!
//! - `DerivesFrom`: object was produced from its parents' state.
//! - `DependsOn`: object requires the parent to remain reconstructible.
//! - `Supersedes`: object makes the parent obsolete for recovery purposes.
//! - `Duplicates`: object carries the same content identity.
//!
//! Reclamation safety is computed from dependency edges and recomputation
//! capability, not from reference counts alone.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::{ReclaimError, Result};

/// Edge kinds in the lineage DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EdgeKind {
    DerivesFrom,
    DependsOn,
    Supersedes,
    Duplicates,
}

impl EdgeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EdgeKind::DerivesFrom => "DERIVES_FROM",
            EdgeKind::DependsOn => "DEPENDS_ON",
            EdgeKind::Supersedes => "SUPERSEDES",
            EdgeKind::Duplicates => "DUPLICATES",
        }
    }
}

/// A typed edge in the lineage graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LineageEdge {
    pub parent: Uuid,
    pub child: Uuid,
    pub kind: EdgeKind,
}

/// Full in-memory lineage graph snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LineageGraph {
    /// parent -> children
    pub parents: HashMap<Uuid, Vec<LineageEdge>>,
    /// child -> parents
    pub children: HashMap<Uuid, Vec<LineageEdge>>,
    pub object_ids: HashSet<Uuid>,
}

impl LineageGraph {
    pub fn add_object(&mut self, id: Uuid) {
        self.object_ids.insert(id);
    }

    /// Add an edge, ensuring DAG acyclicity.
    pub fn add_edge(&mut self, parent: Uuid, child: Uuid, kind: EdgeKind) -> Result<()> {
        if parent == child {
            return Err(ReclaimError::DependencyViolation(
                "self-edge is not allowed".into(),
            ));
        }
        if !self.object_ids.contains(&parent) || !self.object_ids.contains(&child) {
            return Err(ReclaimError::DependencyViolation(
                "edge endpoints must be registered objects".into(),
            ));
        }
        // The persistent representation has the same uniqueness constraint.
        // Keep the in-memory graph idempotent as well so repeated requests do
        // not inflate counts or make validation disagree with the store.
        if self
            .parents
            .get(&parent)
            .is_some_and(|edges| edges.iter().any(|e| e.child == child && e.kind == kind))
        {
            return Ok(());
        }
        // Acyclicity: adding parent->child must not create a cycle. A cycle
        // exists iff child can already reach parent.
        if self.reaches(child, parent) {
            return Err(ReclaimError::DependencyViolation(format!(
                "cycle would be created by edge {parent}->{child}"
            )));
        }
        let edge = LineageEdge {
            parent,
            child,
            kind,
        };
        self.parents.entry(parent).or_default().push(edge.clone());
        self.children.entry(child).or_default().push(edge);
        Ok(())
    }

    pub fn remove_edge(&mut self, parent: Uuid, child: Uuid, kind: EdgeKind) {
        if let Some(v) = self.parents.get_mut(&parent) {
            v.retain(|e| !(e.child == child && e.kind == kind));
        }
        if let Some(v) = self.children.get_mut(&child) {
            v.retain(|e| !(e.parent == parent && e.kind == kind));
        }
    }

    /// BFS reachability from `start` to `target`.
    pub fn reaches(&self, start: Uuid, target: Uuid) -> bool {
        let mut seen = HashSet::new();
        let mut q = VecDeque::new();
        q.push_back(start);
        seen.insert(start);
        while let Some(n) = q.pop_front() {
            if n == target {
                return true;
            }
            if let Some(children) = self.parents.get(&n) {
                for e in children {
                    if seen.insert(e.child) {
                        q.push_back(e.child);
                    }
                }
            }
        }
        false
    }

    /// All descendants of `start` (BFS, excludes start).
    pub fn descendants(&self, start: Uuid) -> HashSet<Uuid> {
        let mut out = HashSet::new();
        let mut q = VecDeque::new();
        q.push_back(start);
        while let Some(n) = q.pop_front() {
            if let Some(children) = self.parents.get(&n) {
                for e in children {
                    if out.insert(e.child) {
                        q.push_back(e.child);
                    }
                }
            }
        }
        out
    }

    /// All ancestors of `start` (BFS, excludes start).
    pub fn ancestors(&self, start: Uuid) -> HashSet<Uuid> {
        let mut out = HashSet::new();
        let mut q = VecDeque::new();
        q.push_back(start);
        while let Some(n) = q.pop_front() {
            if let Some(parents) = self.children.get(&n) {
                for e in parents {
                    if out.insert(e.parent) {
                        q.push_back(e.parent);
                    }
                }
            }
        }
        out
    }

    /// Orphan detection: objects with no registered parents and no registered
    /// children are fine; orphans are *children whose parents are not
    /// registered objects* — enforced at edge insertion. This returns objects
    /// that exist in edges but not in `object_ids`.
    pub fn orphans(&self) -> Vec<Uuid> {
        let mut set = HashSet::new();
        for (p, edges) in &self.parents {
            if !self.object_ids.contains(p) {
                set.insert(*p);
            }
            for e in edges {
                if !self.object_ids.contains(&e.child) {
                    set.insert(e.child);
                }
            }
        }
        for (c, edges) in &self.children {
            if !self.object_ids.contains(c) {
                set.insert(*c);
            }
            for e in edges {
                if !self.object_ids.contains(&e.parent) {
                    set.insert(e.parent);
                }
            }
        }
        let mut v: Vec<Uuid> = set.into_iter().collect();
        v.sort();
        v
    }

    /// Dependency safety: can `candidate` be reclaimed without invalidating a
    /// non-reconstructible dependent?
    ///
    /// Rule (spec headline invariant): an object cannot be reclaimed if doing
    /// so would invalidate a non-reconstructible dependent. A live,
    /// non-reconstructible node blocks reclamation when it is reachable from
    /// the candidate through a path consisting entirely of `DEPENDS_ON`
    /// edges (any `DERIVES_FROM`/`SUPERSEDES`/`DUPLICATES` hop breaks the
    /// dependency chain). Dead (already reclaimed) nodes never block.
    pub fn dependency_safe(
        &self,
        candidate: Uuid,
        reconstructible: &dyn Fn(Uuid) -> bool,
        dead_nodes: &HashSet<Uuid>,
    ) -> Result<()> {
        let mut seen = HashSet::new();
        let mut q = VecDeque::new();
        q.push_back(candidate);
        seen.insert(candidate);
        while let Some(node) = q.pop_front() {
            if let Some(edges) = self.parents.get(&node) {
                for e in edges {
                    // Once a path takes a non-DEPENDS_ON edge it can never
                    // become a pure dependency path again, so do not let that
                    // branch mark nodes as seen. Otherwise an insertion-order
                    // dependent traversal can hide the same node reached by a
                    // later all-DEPENDS_ON path.
                    if e.kind != EdgeKind::DependsOn {
                        continue;
                    }
                    if !seen.insert(e.child) {
                        continue;
                    }
                    if !dead_nodes.contains(&e.child) && !reconstructible(e.child) {
                        return Err(ReclaimError::DependencyViolation(format!(
                            "dependent {} is not reconstructible and requires {}",
                            e.child, candidate
                        )));
                    }
                    q.push_back(e.child);
                }
            }
        }
        Ok(())
    }

    /// Supersession frontier: given a set of object ids, return those that are
    /// superseded by another object in the set. A superseded object may be
    /// reclaimable if the superseder provides a valid recovery frontier.
    pub fn superseded(&self, ids: &HashSet<Uuid>) -> HashSet<Uuid> {
        let mut out = HashSet::new();
        for id in ids {
            if let Some(children) = self.parents.get(id) {
                for e in children {
                    if e.kind == EdgeKind::Supersedes && ids.contains(&e.child) {
                        out.insert(*id);
                    }
                }
            }
        }
        out
    }

    /// Ancestor counts for shared-parent analysis (children referencing
    /// `parent` through DEPENDS_ON edges).
    pub fn dependent_count(&self, parent: Uuid) -> usize {
        self.parents
            .get(&parent)
            .map(|v| v.iter().filter(|e| e.kind == EdgeKind::DependsOn).count())
            .unwrap_or(0)
    }

    /// Validate the whole graph after restart: no orphans, no cycles.
    pub fn validate(&self) -> Result<()> {
        let orphans = self.orphans();
        if !orphans.is_empty() {
            return Err(ReclaimError::Recovery(format!(
                "lineage validation failed: orphan objects {:?}",
                orphans
            )));
        }
        // Both adjacency indexes are public for snapshot serialization. Treat
        // disagreement between them, malformed map keys, or duplicate rows as
        // corruption rather than validating only whichever index a caller
        // happened to consult.
        let mut parent_edges = BTreeSet::new();
        let mut parent_edge_count = 0usize;
        for (parent, edges) in &self.parents {
            for edge in edges {
                parent_edge_count += 1;
                if edge.parent != *parent {
                    return Err(ReclaimError::Recovery(format!(
                        "lineage validation failed: parent index key {parent} contains edge from {}",
                        edge.parent
                    )));
                }
                parent_edges.insert(edge.clone());
            }
        }
        if parent_edges.len() != parent_edge_count {
            return Err(ReclaimError::Recovery(
                "lineage validation failed: duplicate edge in parent index".into(),
            ));
        }

        let mut child_edges = BTreeSet::new();
        let mut child_edge_count = 0usize;
        for (child, edges) in &self.children {
            for edge in edges {
                child_edge_count += 1;
                if edge.child != *child {
                    return Err(ReclaimError::Recovery(format!(
                        "lineage validation failed: child index key {child} contains edge to {}",
                        edge.child
                    )));
                }
                child_edges.insert(edge.clone());
            }
        }
        if child_edges.len() != child_edge_count {
            return Err(ReclaimError::Recovery(
                "lineage validation failed: duplicate edge in child index".into(),
            ));
        }
        if parent_edges != child_edges {
            return Err(ReclaimError::Recovery(
                "lineage validation failed: parent/child indexes disagree".into(),
            ));
        }

        // Full cycle check via topological sort (Kahn's algorithm).
        let mut indegree: HashMap<Uuid, usize> = HashMap::new();
        for id in &self.object_ids {
            indegree.insert(*id, 0);
        }
        for edges in self.parents.values() {
            for e in edges {
                *indegree.entry(e.child).or_insert(0) += 1;
            }
        }
        let mut q: VecDeque<Uuid> = indegree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(k, _)| *k)
            .collect();
        let mut visited = 0usize;
        while let Some(n) = q.pop_front() {
            visited += 1;
            if let Some(children) = self.parents.get(&n) {
                for e in children {
                    let d = indegree.get_mut(&e.child).expect("indegree present");
                    *d -= 1;
                    if *d == 0 {
                        q.push_back(e.child);
                    }
                }
            }
        }
        if visited != self.object_ids.len() {
            return Err(ReclaimError::Recovery(format!(
                "lineage validation failed: cycle detected (visited {visited}/{} objects)",
                self.object_ids.len()
            )));
        }
        Ok(())
    }

    /// The set of ids currently participating in any edge.
    pub fn edge_participants(&self) -> HashSet<Uuid> {
        let mut set = HashSet::new();
        for (p, edges) in &self.parents {
            set.insert(*p);
            for e in edges {
                set.insert(e.child);
            }
        }
        set
    }

    /// All edges, sorted for deterministic serialization.
    pub fn all_edges(&self) -> Vec<LineageEdge> {
        let mut v: Vec<LineageEdge> = self
            .parents
            .values()
            .flat_map(|v| v.iter().cloned())
            .collect();
        v.sort_by(|a, b| (a.parent, a.child, a.kind).cmp(&(b.parent, b.child, b.kind)));
        v
    }

    /// Neighbors of a single object (parents and children), for CLI output.
    pub fn neighbors(&self, id: Uuid) -> Vec<LineageEdge> {
        let mut v: Vec<LineageEdge> = Vec::new();
        if let Some(edges) = self.parents.get(&id) {
            v.extend(edges.iter().cloned());
        }
        if let Some(edges) = self.children.get(&id) {
            v.extend(edges.iter().cloned());
        }
        v.sort_by(|a, b| (a.parent, a.child, a.kind).cmp(&(b.parent, b.child, b.kind)));
        v
    }

    /// Total edge count for stats.
    pub fn edge_count(&self) -> usize {
        self.parents.values().map(|v| v.len()).sum()
    }
}

impl EdgeKind {
    /// Deterministic ordering for serialization.
    pub fn rank(&self) -> u8 {
        match self {
            EdgeKind::DerivesFrom => 0,
            EdgeKind::DependsOn => 1,
            EdgeKind::Supersedes => 2,
            EdgeKind::Duplicates => 3,
        }
    }
}

impl PartialOrd for LineageEdge {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LineageEdge {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.parent, self.child, self.kind.rank()).cmp(&(
            other.parent,
            other.child,
            other.kind.rank(),
        ))
    }
}

/// Collect duplicate-content groups from a content hash mapping.
pub fn duplicate_groups(
    by_hash: &HashMap<Uuid, Option<crate::integrity::ContentHash>>,
) -> Vec<Vec<Uuid>> {
    let mut groups: HashMap<crate::integrity::ContentHash, Vec<Uuid>> = HashMap::new();
    for (id, hash) in by_hash {
        if let Some(h) = hash {
            groups.entry(*h).or_default().push(*id);
        }
    }
    let mut out: Vec<Vec<Uuid>> = groups.into_values().filter(|v| v.len() > 1).collect();
    for g in &mut out {
        g.sort();
    }
    out.sort();
    out
}

/// Sorted helper for deterministic output.
pub fn sorted_uuids(set: &HashSet<Uuid>) -> BTreeSet<Uuid> {
    set.iter().copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycle_rejected() {
        let mut g = LineageGraph::default();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        g.add_object(a);
        g.add_object(b);
        g.add_edge(a, b, EdgeKind::DependsOn).unwrap();
        assert!(g.add_edge(b, a, EdgeKind::DependsOn).is_err());
    }

    #[test]
    fn self_edge_rejected() {
        let mut g = LineageGraph::default();
        let a = Uuid::new_v4();
        g.add_object(a);
        assert!(g.add_edge(a, a, EdgeKind::DependsOn).is_err());
    }

    #[test]
    fn dependency_safety_blocks_non_reconstructible() {
        let mut g = LineageGraph::default();
        let cand = Uuid::new_v4();
        let dep = Uuid::new_v4();
        g.add_object(cand);
        g.add_object(dep);
        g.add_edge(cand, dep, EdgeKind::DependsOn).unwrap();
        let all_reconstructible = |_id: Uuid| true;
        assert!(g
            .dependency_safe(cand, &all_reconstructible, &HashSet::new())
            .is_ok());
        let none_reconstructible = |_id: Uuid| false;
        assert!(g
            .dependency_safe(cand, &none_reconstructible, &HashSet::new())
            .is_err());
        // Reclaiming the dependent itself is fine.
        assert!(g
            .dependency_safe(dep, &none_reconstructible, &HashSet::new())
            .is_ok());
    }

    #[test]
    fn dependency_safety_transitive_depends_on_blocks() {
        let mut g = LineageGraph::default();
        let cand = Uuid::new_v4();
        let mid = Uuid::new_v4();
        let leaf = Uuid::new_v4();
        g.add_object(cand);
        g.add_object(mid);
        g.add_object(leaf);
        g.add_edge(cand, mid, EdgeKind::DependsOn).unwrap();
        g.add_edge(mid, leaf, EdgeKind::DerivesFrom).unwrap();
        // mid is on a DEPENDS_ON path from cand and is non-reconstructible.
        let leaf_reconstructible = |id: Uuid| id == leaf;
        assert!(g
            .dependency_safe(cand, &leaf_reconstructible, &HashSet::new())
            .is_err());
        // Everything non-reconstructible on the path -> block.
        assert!(g
            .dependency_safe(cand, &|_| false, &HashSet::new())
            .is_err());
        // A dead (already reclaimed) dependent does not block.
        let dead: HashSet<Uuid> = [mid].into_iter().collect();
        assert!(g.dependency_safe(cand, &|_| false, &dead).is_ok());
        // Path without DEPENDS_ON never blocks.
        let mut g2 = LineageGraph::default();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        g2.add_object(a);
        g2.add_object(b);
        g2.add_edge(a, b, EdgeKind::DerivesFrom).unwrap();
        assert!(g2.dependency_safe(a, &|_| false, &HashSet::new()).is_ok());
    }

    #[test]
    fn non_dependency_path_cannot_hide_later_dependency_path() {
        let mut g = LineageGraph::default();
        let candidate = Uuid::new_v4();
        let dependent = Uuid::new_v4();
        let intermediate = Uuid::new_v4();
        for id in [candidate, dependent, intermediate] {
            g.add_object(id);
        }

        // Insert the non-dependency path first. The old node-only visited set
        // reached `dependent` through this path and then skipped the later,
        // unsafe all-DEPENDS_ON path.
        g.add_edge(candidate, dependent, EdgeKind::DerivesFrom)
            .unwrap();
        g.add_edge(candidate, intermediate, EdgeKind::DependsOn)
            .unwrap();
        g.add_edge(intermediate, dependent, EdgeKind::DependsOn)
            .unwrap();

        let reconstructible = |id: Uuid| id == intermediate;
        assert!(g
            .dependency_safe(candidate, &reconstructible, &HashSet::new())
            .is_err());
    }

    #[test]
    fn derives_from_does_not_block() {
        let mut g = LineageGraph::default();
        let cand = Uuid::new_v4();
        let child = Uuid::new_v4();
        g.add_object(cand);
        g.add_object(child);
        g.add_edge(cand, child, EdgeKind::DerivesFrom).unwrap();
        let none_reconstructible = |_id: Uuid| false;
        assert!(g
            .dependency_safe(cand, &none_reconstructible, &HashSet::new())
            .is_ok());
    }

    #[test]
    fn supersession_detection() {
        let mut g = LineageGraph::default();
        let v10 = Uuid::new_v4();
        let v11 = Uuid::new_v4();
        g.add_object(v10);
        g.add_object(v11);
        g.add_edge(v10, v11, EdgeKind::Supersedes).unwrap();
        let ids: HashSet<Uuid> = [v10, v11].into_iter().collect();
        let sup = g.superseded(&ids);
        assert!(sup.contains(&v10));
        assert!(!sup.contains(&v11));
    }

    #[test]
    fn shared_parents_counted() {
        let mut g = LineageGraph::default();
        let parent = Uuid::new_v4();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        g.add_object(parent);
        g.add_object(a);
        g.add_object(b);
        g.add_edge(parent, a, EdgeKind::DependsOn).unwrap();
        g.add_edge(parent, b, EdgeKind::DependsOn).unwrap();
        assert_eq!(g.dependent_count(parent), 2);
    }

    #[test]
    fn orphans_detected() {
        let mut g = LineageGraph::default();
        let missing = Uuid::new_v4();
        let child = Uuid::new_v4();
        g.add_object(child);
        // Manually inject an edge to a missing parent (simulating corrupt store).
        g.children.entry(child).or_default().push(LineageEdge {
            parent: missing,
            child,
            kind: EdgeKind::DependsOn,
        });
        let orphans = g.orphans();
        assert!(orphans.contains(&missing));
    }

    #[test]
    fn graph_validation_catches_injected_cycle() {
        let mut g = LineageGraph::default();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        g.add_object(a);
        g.add_object(b);
        g.add_edge(a, b, EdgeKind::DerivesFrom).unwrap();
        // Inject a cycle bypassing add_edge guards to simulate corrupted store.
        let edge = LineageEdge {
            parent: b,
            child: a,
            kind: EdgeKind::DependsOn,
        };
        g.parents.entry(b).or_default().push(edge.clone());
        g.children.entry(a).or_default().push(edge);
        assert!(g.validate().is_err());
    }

    #[test]
    fn graph_validation_passes_on_clean_dag() {
        let mut g = LineageGraph::default();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        g.add_object(a);
        g.add_object(b);
        g.add_object(c);
        g.add_edge(a, b, EdgeKind::DerivesFrom).unwrap();
        g.add_edge(a, c, EdgeKind::DependsOn).unwrap();
        g.validate().unwrap();
    }

    #[test]
    fn graph_validation_accepts_parallel_typed_edges() {
        let mut g = LineageGraph::default();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        g.add_object(a);
        g.add_object(b);
        g.add_edge(a, b, EdgeKind::DerivesFrom).unwrap();
        g.add_edge(a, b, EdgeKind::DependsOn).unwrap();
        // Repeating the exact edge is idempotent, like INSERT OR IGNORE in
        // the persistent representation.
        g.add_edge(a, b, EdgeKind::DependsOn).unwrap();
        assert_eq!(g.edge_count(), 2);
        g.validate().unwrap();
    }

    #[test]
    fn graph_validation_rejects_disagreeing_adjacency_indexes() {
        let mut g = LineageGraph::default();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        g.add_object(a);
        g.add_object(b);
        g.parents.entry(a).or_default().push(LineageEdge {
            parent: a,
            child: b,
            kind: EdgeKind::DependsOn,
        });
        assert!(g.validate().is_err());
    }

    #[test]
    fn duplicate_groups_detected() {
        let mut by_hash = HashMap::new();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        let h = crate::integrity::ContentHash::of(b"same");
        by_hash.insert(a, Some(h));
        by_hash.insert(b, Some(h));
        by_hash.insert(c, None);
        let groups = duplicate_groups(&by_hash);
        assert_eq!(groups.len(), 1);
        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!(groups[0], expected);
    }

    #[test]
    fn descendants_and_ancestors() {
        let mut g = LineageGraph::default();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        g.add_object(a);
        g.add_object(b);
        g.add_object(c);
        g.add_edge(a, b, EdgeKind::DerivesFrom).unwrap();
        g.add_edge(b, c, EdgeKind::DependsOn).unwrap();
        assert_eq!(g.descendants(a), HashSet::from([b, c]));
        assert_eq!(g.ancestors(c), HashSet::from([a, b]));
        assert!(g.reaches(a, c));
        assert!(!g.reaches(c, a));
    }
}
