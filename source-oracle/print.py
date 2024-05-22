import pstats
from pstats import SortKey
p = pstats.Stats('profile')
p.strip_dirs()
p.sort_stats(SortKey.TIME).print_stats(10)

p.print_stats(10)
