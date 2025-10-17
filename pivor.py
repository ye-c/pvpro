from app.core import Pivor, compare_stats

pv = Pivor()

# pv.recover()
# exit()

stats1 = pv.stats()

pv.fit(handle_duplicate=False, work_dir=None)

stats2 = pv.stats()
res = compare_stats(stats1, stats2)

print(res.to_markdown())
