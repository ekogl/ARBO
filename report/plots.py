import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Set the style for nice looking plots
sns.set_theme(style="whitegrid")

# Data from the LaTeX table
s = np.array([1, 2, 4, 6, 8])
T_actual = np.array([292, 150, 83, 65, 55])
# For s=1, T_pred is not defined/equal to T_actual. Let's use 292 for plotting continuity or NaN.
T_pred = np.array([292, 146, 73, 49, 37]) 
R_s = np.array([0, 4, 10, 16, 18]) # 0 for s=1
Cost = np.array([292, 300, 332, 390, 440])
Speedup = np.array([1.0, 1.95, 3.52, 4.49, 5.3])

# Create a figure with 2x2 subplots
fig, axs = plt.subplots(2, 2, figsize=(14, 10))
# fig.suptitle('Parallel Performance Analysis', fontsize=18, fontweight='bold')

# Plot 1: Execution Time
axs[0, 0].plot(s, T_actual, marker='o', linestyle='-', color='b', linewidth=2, markersize=8, label='$T_{actual}$')
axs[0, 0].plot(s, T_pred, marker='s', linestyle='--', color='g', linewidth=2, markersize=8, label='$T_{pred}$')
axs[0, 0].set_title('Execution Time vs. Pods (s)', fontsize=14)
axs[0, 0].set_xlabel('Number of Pods (s)', fontsize=12)
axs[0, 0].set_ylabel('Time (seconds)', fontsize=12)
axs[0, 0].legend(fontsize=12)
axs[0, 0].grid(True)

# Plot 2: Speedup
axs[0, 1].plot(s, Speedup, marker='o', linestyle='-', color='purple', linewidth=2, markersize=8, label='Actual Speedup')
axs[0, 1].plot(s, s, linestyle=':', color='gray', linewidth=2, label='Ideal Speedup ($y=x$)')
axs[0, 1].set_title('Speedup vs. Pods (s)', fontsize=14)
axs[0, 1].set_xlabel('Number of Pods (s)', fontsize=12)
axs[0, 1].set_ylabel('Speedup', fontsize=12)
axs[0, 1].legend(fontsize=12)
axs[0, 1].grid(True)

# Plot 3: Cost
axs[1, 0].bar(s, Cost, color='orange', alpha=0.7, width=0.8, edgecolor='black')
axs[1, 0].set_title('Total Cost vs. Pods (s)', fontsize=14)
axs[1, 0].set_xlabel('Number of Pods (s)', fontsize=12)
axs[1, 0].set_ylabel('Cost', fontsize=12)
axs[1, 0].set_xticks(s)
axs[1, 0].grid(axis='y')

# Plot 4: Overhead R(s)
axs[1, 1].bar(s, R_s, color='salmon', alpha=0.7, width=0.8, edgecolor='black')
axs[1, 1].set_title('Parallel Overhead $R(s)$ vs. Pods (s)', fontsize=14)
axs[1, 1].set_xlabel('Number of Pods (s)', fontsize=12)
axs[1, 1].set_ylabel('Overhead Time', fontsize=12)
axs[1, 1].set_xticks(s)
axs[1, 1].grid(axis='y')

plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Adjust layout to make room for suptitle
plt.savefig('plots/parallel_performance_plot.png', dpi=300)
