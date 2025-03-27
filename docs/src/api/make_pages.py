"""Generate the api pages and navigation."""

import os
from pathlib import Path

import mkdocs_gen_files

package = os.getenv("PACKAGE")
nav = mkdocs_gen_files.Nav()
for path in sorted(Path(package).glob("**/*.py")):
    with mkdocs_gen_files.open(f"api/{path.with_suffix('')}.md", "w") as f:
        module_path = ".".join(
            [p for p in path.with_suffix("").parts if p != "__init__"]
        )
        print(f"::: {module_path}", file=f)
    nav[path.parts] = f"{path.with_suffix('')}.md"

with mkdocs_gen_files.open("api/navigation.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
