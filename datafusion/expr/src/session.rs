// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Session provider abstraction.
//!
//! Session-bound scalar UDFs (advisory locks, `current_user`, `now()`,
//! `current_setting`, `set_config`, ...) need per-session state at invoke
//! time. Baking session state into the UDF's `self` makes the resulting
//! `Arc<ScalarUDF>` per-session, which breaks plan caching — any cached
//! logical plan embeds the first session's UDF instance, and later sessions
//! hitting the cache get stale state.
//!
//! `SessionProvider` decouples the UDF from the session: embedding DBs
//! implement this trait on their own session type, plumb an
//! `Arc<dyn SessionProvider>` into [`crate::execution_props::ExecutionProps`],
//! and it flows through [`crate::udf::ScalarFunctionArgs::session`] at
//! invoke time. UDFs stay stateless singletons safe to cache.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;

use datafusion_common::Result;

/// Ambient per-session context available to scalar UDFs at invoke time.
///
/// Implementors represent "the session this query is executing on behalf of".
/// The trait methods expose the subset of session state that any
/// session-bound scalar function might need to consult. Each embedding
/// database implements the trait on its own session type; DataFusion never
/// constructs an instance directly.
///
/// The returned references point at data held inside the session and are
/// expected to be cheap to obtain (clones of `Arc`s, direct field reads).
/// Implementations must be `Send + Sync` because `Arc<dyn SessionProvider>`
/// is threaded through physical expression evaluation.
pub trait SessionProvider: Debug + Send + Sync {
    /// Opaque session identifier used by session-scoped primitives (e.g.,
    /// advisory lock registries keyed by session). Must be non-zero for an
    /// active session; zero is reserved for "no session".
    fn session_id(&self) -> u64;

    /// IANA timezone name (or fixed offset like "+05:30") used for
    /// `now()`-family UDFs and for displaying `timestamptz` values.
    fn timezone(&self) -> &str;

    /// Authenticated role name for the session.
    fn current_user(&self) -> &str;

    /// Name of the database the session is connected to.
    fn current_database(&self) -> &str;

    /// Whether the current role is a superuser.
    fn is_superuser(&self) -> bool;

    /// Currently active transaction identifier, if the session is inside a
    /// transaction block; `None` otherwise.
    fn active_txn_id(&self) -> Option<u64>;

    /// Schema search path in order of precedence.
    fn search_path(&self) -> &[String];

    /// Deadline after which the currently executing statement should abort.
    /// Returns `None` when no deadline is set for this statement.
    fn statement_deadline(&self) -> Option<Instant>;

    /// Read a session configuration variable by name (case-insensitive).
    /// Returns the current string value, or `None` if the variable is not
    /// set on the session.
    fn session_var(&self, name: &str) -> Option<String>;

    /// Set a session configuration variable. `is_local = true` scopes the
    /// change to the current transaction (rolled back on ROLLBACK).
    /// Returns the normalized value that was stored, or an error if the
    /// variable name/value is rejected.
    fn set_session_var(&self, name: &str, value: &str, is_local: bool)
        -> Result<String>;

    /// Raw pointer to the session's execution runtime context, used by
    /// PL/pgSQL UDFs that need exception-block savepoint integration
    /// (deferred constraint truncation, cascade queue rollback, FK
    /// probe cache cleanup). The caller casts to the concrete type.
    /// Returns null when no runtime context is available.
    fn runtime_ctx_ptr(&self) -> *mut () {
        std::ptr::null_mut()
    }
}

/// A no-op `SessionProvider` useful for tests, ad-hoc expression evaluation
/// outside a user session, and DataFusion's own test suite. Every method
/// returns an empty / zero / sentinel value; session-bound UDFs invoked
/// through this provider are expected to fail loudly.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSession;

impl SessionProvider for NoSession {
    fn session_id(&self) -> u64 {
        0
    }
    fn timezone(&self) -> &str {
        "UTC"
    }
    fn current_user(&self) -> &str {
        ""
    }
    fn current_database(&self) -> &str {
        ""
    }
    fn is_superuser(&self) -> bool {
        false
    }
    fn active_txn_id(&self) -> Option<u64> {
        None
    }
    fn search_path(&self) -> &[String] {
        &[]
    }
    fn statement_deadline(&self) -> Option<Instant> {
        None
    }
    fn session_var(&self, _name: &str) -> Option<String> {
        None
    }
    fn set_session_var(
        &self,
        _name: &str,
        _value: &str,
        _is_local: bool,
    ) -> Result<String> {
        Err(datafusion_common::DataFusionError::Execution(
            "no active session: set_session_var is not supported on NoSession"
                .to_string(),
        ))
    }
}

/// Convenience: an `Arc<dyn SessionProvider>` wrapping a shared
/// [`NoSession`] singleton, returned by default constructors.
pub fn no_session() -> Arc<dyn SessionProvider> {
    Arc::new(NoSession)
}
