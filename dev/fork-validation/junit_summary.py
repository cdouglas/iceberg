#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Summarizes JUnit XML under a run directory, or diffs two runs class by class.

Iceberg's parameterized tests report only their parameters as the test name, so
test cases are not unique by name. Runs are compared on per-class counts, and
failures name their method from the stack trace.

A run is a directory of JUnit XML as run-suite.sh keeps it, or a CSV of
per-class counts that --export wrote from one. A class counts as worse when it
passes fewer cases, fails more, or skips more than in the base.

usage: python3 -I junit_summary.py <run>
       python3 -I junit_summary.py <base-run> <new-run>
       python3 -I junit_summary.py --export <run-dir> <out.csv>
"""

import csv
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path


def module_of(path, root):
    rel = path.relative_to(root).parts
    return "/".join(rel[: rel.index("build")])


def failed_method(case, classname):
    node = case.find("failure")
    if node is None:
        node = case.find("error")
    trace = (node.text or "") + (node.get("message") or "")
    match = re.search(re.escape(classname) + r"\.(\w+)\(", trace)
    first = (node.get("message") or "").splitlines()[:1]
    return (match.group(1) if match else "?"), (first[0][:160] if first else "")


def load(root):
    root = Path(root)
    if root.suffix == ".csv":
        return load_csv(root)
    counts = {}
    failures = []
    for xml in root.rglob("build/test-results/test/*.xml"):
        module = module_of(xml, root)
        for case in ET.parse(xml).getroot().iter("testcase"):
            classname = case.get("classname")
            key = f"{module}::{classname}"
            if case.find("failure") is not None or case.find("error") is not None:
                status = "fail"
                method, message = failed_method(case, classname)
                failures.append(f"{key}.{method} [{case.get('name')[:80]}] {message}")
            elif case.find("skipped") is not None:
                status = "skip"
            else:
                status = "pass"
            counts.setdefault(key, Counter())[status] += 1
    return counts, failures


def load_csv(path):
    counts = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            key = f"{row['module']}::{row['class']}"
            counts[key] = Counter(
                {status: int(row[status]) for status in ("pass", "fail", "skip")})
    return counts, []


def export(counts, out):
    with open(out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["module", "class", "pass", "fail", "skip"])
        for key in sorted(counts):
            module, classname = key.split("::")
            c = counts[key]
            writer.writerow([module, classname, c["pass"], c["fail"], c["skip"]])


def summarize(counts, failures):
    per_module = {}
    for key, c in counts.items():
        per_module.setdefault(key.split("::")[0], Counter()).update(c)
    for module in sorted(per_module):
        c = per_module[module]
        print(f"{module:40} pass={c['pass']:6} fail={c['fail']:4} skip={c['skip']:5}")
    print(f"\n{len(failures)} failing test cases")
    for f in sorted(failures):
        print("  " + f)


def diff(base, new):
    (base_counts, _), (new_counts, new_failures) = base, new
    worse, missing = [], []
    for key, b in sorted(base_counts.items()):
        n = new_counts.get(key)
        if n is None:
            if b["pass"]:
                missing.append(f"{key} (base pass={b['pass']})")
        elif n["pass"] < b["pass"] or n["fail"] > b["fail"] or n["skip"] > b["skip"]:
            worse.append(f"{key}: pass {b['pass']}->{n['pass']} fail {b['fail']}->{n['fail']} "
                         f"skip {b['skip']}->{n['skip']}")
    added = sorted(k for k in new_counts if k not in base_counts)
    for title, items in [
        ("classes that got worse", worse),
        ("classes that passed in base and are missing from new", missing),
        ("classes only in new", added),
        ("failing test cases in new", sorted(new_failures)),
    ]:
        print(f"== {title}: {len(items)}")
        for item in items:
            print("  " + item)


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
    elif args[0] == "--export" and len(args) == 3:
        export(load(args[1])[0], args[2])
    elif len(args) == 1:
        summarize(*load(args[0]))
    elif len(args) == 2:
        diff(load(args[0]), load(args[1]))
    else:
        sys.exit(__doc__)
