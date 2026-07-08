# src/my_project/__init__.py
import dunamai as _dunamai

__version__ = _dunamai.get_version(
                                   "bloodhound_gang",
                                   third_choice=_dunamai.Version.from_any_vcs
                                  ).serialize()