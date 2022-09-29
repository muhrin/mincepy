# -*- coding: utf-8 -*-
author_info = (("Martin Uhrin", "martin.uhrin.10@ucl.ac.uk"),)
version_info = (0, 16, 3)

__author__ = ", ".join(f"{info[0]} <{info[1]}>" for info in author_info)
__version__ = ".".join(map(str, version_info))

__all__ = ("__version__",)
