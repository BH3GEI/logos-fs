use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::middleware::VfsMiddleware;
use crate::namespace::Namespace;
use crate::uri;
use crate::VfsError;

/// The VFS routing table — the kernel's central dispatch mechanism.
///
/// Subsystems `mount` during boot. Once all required subsystems are in place,
/// `open` is called and the table starts accepting requests.
///
/// Middleware hooks run before/after operations (RFC 002 §12.6).
pub struct RoutingTable {
    namespaces: HashMap<String, Box<dyn Namespace>>,
    middlewares: Vec<Box<dyn VfsMiddleware>>,
    open: AtomicBool,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self {
            namespaces: HashMap::new(),
            middlewares: Vec::new(),
            open: AtomicBool::new(false),
        }
    }

    /// Mount a namespace handler. Must be called before `open`.
    pub fn mount(&mut self, ns: Box<dyn Namespace>) {
        let name = ns.name().to_string();
        self.namespaces.insert(name, ns);
    }

    /// Add a middleware to the chain. Called during boot.
    pub fn add_middleware(&mut self, mw: Box<dyn VfsMiddleware>) {
        self.middlewares.push(mw);
    }

    /// Mark the routing table as open. After this, requests are accepted.
    pub fn open(&self) {
        self.open.store(true, Ordering::Release);
    }

    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// List mounted namespace names.
    pub fn mounted(&self) -> Vec<&str> {
        self.namespaces.keys().map(|s| s.as_str()).collect()
    }

    pub async fn read(&self, raw_uri: &str) -> Result<String, VfsError> {
        for mw in &self.middlewares {
            mw.before_read(raw_uri).await?;
        }
        let (ns, uri) = self.resolve(raw_uri)?;
        let path_refs: Vec<&str> = uri.path.iter().copied().collect();
        let result = ns.read(&path_refs).await;
        let success = result.is_ok();
        for mw in &self.middlewares {
            mw.after_op("read", raw_uri, success).await;
        }
        result
    }

    pub async fn write(&self, raw_uri: &str, content: &str) -> Result<(), VfsError> {
        for mw in &self.middlewares {
            mw.before_write(raw_uri, content).await?;
        }
        let (ns, uri) = self.resolve(raw_uri)?;
        let path_refs: Vec<&str> = uri.path.iter().copied().collect();
        let result = ns.write(&path_refs, content).await;
        let success = result.is_ok();
        for mw in &self.middlewares {
            mw.after_op("write", raw_uri, success).await;
        }
        result
    }

    pub async fn patch(&self, raw_uri: &str, partial: &str) -> Result<(), VfsError> {
        for mw in &self.middlewares {
            mw.before_patch(raw_uri, partial).await?;
        }
        let (ns, uri) = self.resolve(raw_uri)?;
        let path_refs: Vec<&str> = uri.path.iter().copied().collect();
        let result = ns.patch(&path_refs, partial).await;
        let success = result.is_ok();
        for mw in &self.middlewares {
            mw.after_op("patch", raw_uri, success).await;
        }
        result
    }

    fn resolve<'a>(&'a self, raw_uri: &'a str) -> Result<(&'a dyn Namespace, uri::LogosUri<'a>), VfsError> {
        if !self.is_open() {
            return Err(VfsError::NotReady);
        }
        let parsed = uri::parse(raw_uri)?;
        let ns = self
            .namespaces
            .get(parsed.namespace)
            .ok_or_else(|| VfsError::NamespaceNotMounted(parsed.namespace.to_string()))?;
        Ok((ns.as_ref(), parsed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct StubNs;

    #[async_trait]
    impl Namespace for StubNs {
        fn name(&self) -> &str { "stub" }
        async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
            Ok(format!("read:{}", path.join("/")))
        }
        async fn write(&self, _path: &[&str], _content: &str) -> Result<(), VfsError> {
            Ok(())
        }
        async fn patch(&self, _path: &[&str], _partial: &str) -> Result<(), VfsError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn rejects_before_open() {
        let table = RoutingTable::new();
        assert!(matches!(
            table.read("logos://stub/x").await,
            Err(VfsError::NotReady)
        ));
    }

    #[tokio::test]
    async fn routes_after_open() {
        let mut table = RoutingTable::new();
        table.mount(Box::new(StubNs));
        table.open();
        let result = table.read("logos://stub/a/b").await.unwrap();
        assert_eq!(result, "read:a/b");
    }

    #[tokio::test]
    async fn unknown_namespace() {
        let mut table = RoutingTable::new();
        table.mount(Box::new(StubNs));
        table.open();
        assert!(matches!(
            table.read("logos://missing/x").await,
            Err(VfsError::NamespaceNotMounted(_))
        ));
    }
}
