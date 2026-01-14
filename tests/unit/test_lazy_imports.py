"""
Tests for lazy import behavior.

These tests verify that heavy dependencies (networkx, matplotlib, click)
are not loaded until their associated features are accessed.
"""

import sys


def test_lazy_diagram_import():
    """Diagram module should not be loaded until dj.Diagram is accessed."""
    # Remove datajoint from sys.modules to get fresh import
    modules_to_remove = [key for key in sys.modules if key.startswith("datajoint")]
    for mod in modules_to_remove:
        del sys.modules[mod]

    # Import datajoint
    import datajoint as dj

    # Diagram module should not be loaded yet
    assert "datajoint.diagram" not in sys.modules, "diagram module loaded eagerly"

    # Access Diagram - should trigger lazy load
    Diagram = dj.Diagram
    assert "datajoint.diagram" in sys.modules, "diagram module not loaded after access"
    assert Diagram.__name__ == "Diagram"


def test_lazy_admin_import():
    """Admin module should not be loaded until dj.kill is accessed."""
    # Remove datajoint from sys.modules to get fresh import
    modules_to_remove = [key for key in sys.modules if key.startswith("datajoint")]
    for mod in modules_to_remove:
        del sys.modules[mod]

    # Import datajoint
    import datajoint as dj

    # Admin module should not be loaded yet
    assert "datajoint.admin" not in sys.modules, "admin module loaded eagerly"

    # Access kill - should trigger lazy load
    kill = dj.kill
    assert "datajoint.admin" in sys.modules, "admin module not loaded after access"
    assert callable(kill)


def test_lazy_cli_import():
    """CLI module should not be loaded until dj.cli is accessed."""
    # Remove datajoint from sys.modules to get fresh import
    modules_to_remove = [key for key in sys.modules if key.startswith("datajoint")]
    for mod in modules_to_remove:
        del sys.modules[mod]

    # Import datajoint
    import datajoint as dj

    # CLI module should not be loaded yet
    assert "datajoint.cli" not in sys.modules, "cli module loaded eagerly"

    # Access cli - should trigger lazy load and return the function
    cli_func = dj.cli
    assert "datajoint.cli" in sys.modules, "cli module not loaded after access"
    assert callable(cli_func), "dj.cli should be callable (the cli function)"


def test_diagram_module_access():
    """dj.diagram should return the diagram module for accessing module-level attrs."""
    # Remove datajoint from sys.modules to get fresh import
    modules_to_remove = [key for key in sys.modules if key.startswith("datajoint")]
    for mod in modules_to_remove:
        del sys.modules[mod]

    import datajoint as dj

    # Access dj.diagram should return the module
    diagram_module = dj.diagram
    assert hasattr(diagram_module, "diagram_active"), "diagram module should have diagram_active"
    assert hasattr(diagram_module, "Diagram"), "diagram module should have Diagram class"


def test_diagram_aliases():
    """Di and ERD should be aliases for Diagram."""
    # Remove datajoint from sys.modules to get fresh import
    modules_to_remove = [key for key in sys.modules if key.startswith("datajoint")]
    for mod in modules_to_remove:
        del sys.modules[mod]

    import datajoint as dj

    # ERD alias should resolve to Diagram
    assert dj.Diagram is dj.ERD


def test_core_imports_available():
    """Core functionality should be available immediately after import."""
    # Remove datajoint from sys.modules to get fresh import
    modules_to_remove = [key for key in sys.modules if key.startswith("datajoint")]
    for mod in modules_to_remove:
        del sys.modules[mod]

    import datajoint as dj

    # Core classes should be available without triggering lazy loads
    assert hasattr(dj, "Schema")
    assert hasattr(dj, "Table")
    assert hasattr(dj, "Manual")
    assert hasattr(dj, "Lookup")
    assert hasattr(dj, "Computed")
    assert hasattr(dj, "Imported")
    assert hasattr(dj, "Part")
    assert hasattr(dj, "Connection")
    assert hasattr(dj, "config")
    assert hasattr(dj, "errors")

    # Heavy modules should still not be loaded
    assert "datajoint.diagram" not in sys.modules
    assert "datajoint.admin" not in sys.modules
    assert "datajoint.cli" not in sys.modules
