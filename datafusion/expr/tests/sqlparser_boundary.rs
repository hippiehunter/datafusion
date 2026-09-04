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

use std::fs;
use std::path::Path;

fn assert_source_tree_is_parser_free(path: &Path) {
    for entry in fs::read_dir(path).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        if path.is_dir() {
            assert_source_tree_is_parser_free(&path);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let source = fs::read_to_string(&path).expect("read Rust source");
            assert!(
                !source.contains("sqlparser"),
                "semantic expression source {} imports or names sqlparser",
                path.display()
            );
        }
    }
}

#[test]
fn semantic_expression_crate_has_no_parser_dependency() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest =
        fs::read_to_string(crate_root.join("Cargo.toml")).expect("read manifest");
    assert!(
        !manifest.contains("sqlparser"),
        "datafusion-expr must not depend on sqlparser"
    );
    assert_source_tree_is_parser_free(&crate_root.join("src"));
}
