//! Crash injection for the transport tiers.

/// An [`object_store::ObjectStore`] that, once armed, fails the next PUT of a
/// `*.manifest.json` object — the process crash in the window between
/// `upload`'s payload multipart (already complete) and its sibling-manifest
/// PUT. Everything else delegates to the wrapped store.
#[derive(Debug)]
pub struct ManifestPutCrash {
    pub inner: std::sync::Arc<dyn object_store::ObjectStore>,
    pub armed: std::sync::atomic::AtomicBool,
}

impl std::fmt::Display for ManifestPutCrash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ManifestPutCrash({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for ManifestPutCrash {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if location.as_ref().ends_with(".manifest.json")
            && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(object_store::Error::Generic {
                store: "ManifestPutCrash",
                source: "injected crash between payload and manifest PUT".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
