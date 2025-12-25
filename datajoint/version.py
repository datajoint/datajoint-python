"""
Version information for DataJoint.

This module contains the package version, which is auto-managed by GitHub Actions.
"""

from __future__ import annotations

# version bump auto managed by Github Actions:
# label_prs.yaml(prep), release.yaml(bump), post_release.yaml(edit)
# manually set this version will be eventually overwritten by the above actions
__version__ = "0.14.6"

assert len(__version__) <= 10  # The log table limits version to the 10 characters
