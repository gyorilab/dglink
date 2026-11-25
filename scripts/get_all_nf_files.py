"""
List all files of the NF Data Portal with their extensions.

The list is saved in a cached location.
"""

from dglink.core.utils import get_projects_files
from dglink.portals.nf_data_portal import get_all_nf_studies

if __name__ == "__main__":
    projects_ids = get_all_nf_studies()
    get_projects_files(project_ids=projects_ids)
