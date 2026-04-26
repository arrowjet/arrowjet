"""Generate benchmark charts for README and blog post.

Uses the latest validated benchmark numbers:
- Reads: 10M rows, 4-node ra3.large, EC2 same region, 5 iterations randomized
- Writes: 1M rows, 4-node ra3.large, EC2 same region, 6-lane comparison
"""
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import os

output_dir = os.path.dirname(os.path.abspath(__file__))

colors_3 = ['#c0392b', '#e67e22', '#27ae60']

# --- Chart 1: Read benchmark (10M rows, 4-node cluster) ---
labels = ['cursor.fetchall()', 'Manual UNLOAD', 'Arrowjet']
times = [144.54, 58, 36.30]

fig, ax = plt.subplots(figsize=(9, 3.5))
bars = ax.barh(labels, times, color=colors_3, height=0.5, edgecolor='white', linewidth=0.5)
ax.set_xlabel('Seconds (lower is better)', fontsize=11)
ax.set_title('Redshift Read  - 10M rows, 4-node ra3.large cluster', fontsize=13, fontweight='bold', pad=12)
ax.bar_label(bars, labels=['144.5s', '58.0s', '36.3s  (4x faster)'], padding=8, fontsize=11)
ax.set_xlim(0, 200)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.tick_params(axis='y', labelsize=11)
plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'read_benchmark.png'), dpi=200, bbox_inches='tight')
print("Saved: read_benchmark.png")
plt.close()

# --- Chart 2: Write benchmark (1M rows, 6-lane comparison) ---
labels_w = [
    'executemany\n(batch 5000)',
    'Multi-row VALUES\n(batch 5000)',
    'Parallel VALUES\n(4 threads)',
    'Manual COPY',
    'Arrowjet',
]
times_w = [97556, 195.8, 148.4, 4.06, 3.33]
colors_w = ['#c0392b', '#e74c3c', '#e67e22', '#f39c12', '#27ae60']

fig, ax = plt.subplots(figsize=(9, 4.5))
bars = ax.barh(labels_w, times_w, color=colors_w, height=0.55, edgecolor='white', linewidth=0.5)
ax.set_xlabel('Seconds  - log scale (lower is better)', fontsize=11)
ax.set_title('Redshift Write  - 1M rows, 4-node ra3.large cluster', fontsize=13, fontweight='bold', pad=12)
ax.set_xscale('log')
ax.bar_label(bars, labels=['27.1 hours', '195.8s', '148.4s', '4.06s', '3.33s  (baseline)'], padding=8, fontsize=11)
ax.set_xlim(1, 400000)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.tick_params(axis='y', labelsize=11)
plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'write_benchmark.png'), dpi=200, bbox_inches='tight')
print("Saved: write_benchmark.png")
plt.close()

print("Done.")
