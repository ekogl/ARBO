import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns

# ---------------------------------------------------------
# 1. Define the Data
# ---------------------------------------------------------
# Based on the LaTeX table provided
data = {
    'Config_ID': range(1, 11),
    's_all': [1, 2, 2, 3, 3, 3, 4, 4, 4, 4],
    's_afr': [1, 1, 2, 1, 2, 3, 1, 2, 3, 4],
    
    # ALL Task Metrics
    'T_all':      [101, 72, 76, 60, 65, 67, 54, 46, 50, 48],
    'T_pred_all': [np.nan, 50, 50, 34, 34, 34, 25, 25, 25, 25], # nan for '-'
    'R_all':      [0, 22, 26, 26, 31, 33, 29, 21, 25, 23],      # 0 for '-'
    
    # AFR Task Metrics
    'T_afr':      [55, 64, 57, 66, 60, 50, 69, 50, 42, 37],
    'T_pred_afr': [np.nan, 55, 28, 55, 28, 14, 55, 28, 18, 14], # nan for '-'
    'R_afr':      [0, 9, 29, 11, 32, 36, 14, 22, 24, 23],       # 0 for '-'
    
    # Global Metrics
    'Cost':    [156, 201, 251, 232, 294, 324, 265, 258, 277, 297],
    'Speedup': [1.00, 1.4, 1.33, 1.53, 1.55, 1.51, 1.46, 2.02, 2.02, 2.1]
}

df = pd.DataFrame(data)

# Calculate Global Time (Bottleneck)
# Based on the bolding in your table, the system time is determined by the slower task (max time).
df['T_actual_global'] = df[['T_all', 'T_afr']].max(axis=1)

# Create a label for each configuration (e.g., "4,2")
df['Label'] = df.apply(lambda row: f"{int(row['s_all'])},{int(row['s_afr'])}", axis=1)

# ---------------------------------------------------------
# 2. Plot: Pareto Frontier (Cost vs Time)
# ---------------------------------------------------------
plt.figure(figsize=(9, 6))
plt.rcParams.update({'font.size': 12})

# Plot points
sns.scatterplot(data=df, x='Cost', y='T_actual_global', s=150, color='#1f77b4', zorder=2, label='Configurations')

# Find pareto optimal points (simple heuristic for visualization line)
# Sort by Cost
df_sorted = df.sort_values('Cost')
plt.plot(df_sorted['Cost'], df_sorted['T_actual_global'], linestyle='--', color='gray', zorder=1, alpha=0.5)

path_labels = ["1,1", "2,1", "3,1", "4,2", "4,3", "4,4"]

path_df = df[df['Label'].isin(path_labels)].set_index('Label').reindex(path_labels).reset_index()

# Plot the path line
plt.plot(path_df['Cost'], path_df['T_actual_global'], color='red', linestyle='-', linewidth=2, alpha=0.8, zorder=2, label='Lower Bound Path')

for i in range(len(path_df) - 1):
    p1 = path_df.iloc[i]
    p2 = path_df.iloc[i+1]
    plt.annotate('', xy=(p2['Cost'], p2['T_actual_global']), xytext=(p1['Cost'], p1['T_actual_global']),
                 arrowprops=dict(arrowstyle='-', color='red', lw=1.5), zorder=2)

# Annotate points
for i, row in df.iterrows():
    plt.annotate(row['Label'], 
                 (row['Cost'], row['T_actual_global']),
                 xytext=(8, 8), textcoords='offset points',
                 fontsize=10,
                 arrowprops=dict(arrowstyle='-', color='black', alpha=0.5))

plt.title('Pareto Front: Cost vs. Time')
plt.xlabel('Global Cost')
plt.ylabel('Global Time ($T = \max(T_{all}, T_{afr})$)')
plt.grid(True, linestyle='--', alpha=0.5)
plt.legend()
plt.tight_layout()
plt.savefig('plots/pareto_frontier_tasks.png', dpi=300)
plt.show()

# ---------------------------------------------------------
# 3. Plot: 2x2 Performance Analysis
# ---------------------------------------------------------
sns.set_theme(style="whitegrid")

fig, axs = plt.subplots(2, 2, figsize=(15, 10))
# fig.suptitle('Detailed Task Performance Analysis', fontsize=18, fontweight='bold')

# X-axis labels (Configurations)
x_labels = df['Label']
x = np.arange(len(x_labels))

# --- Plot 1: Execution Time (ALL vs AFR) ---
# We plot the Actual vs Predicted for both tasks
axs[0, 0].plot(x, df['T_all'], marker='o', label='$T_{all}$ (Actual)', color='tab:blue', linewidth=2)
axs[0, 0].plot(x, df['T_pred_all'], marker='x', linestyle='--', label='$T_{all}$ (Pred)', color='tab:blue', alpha=0.6)
axs[0, 0].plot(x, df['T_afr'], marker='s', label='$T_{afr}$ (Actual)', color='tab:orange', linewidth=2)
axs[0, 0].plot(x, df['T_pred_afr'], marker='+', linestyle='--', label='$T_{afr}$ (Pred)', color='tab:orange', alpha=0.6)

axs[0, 0].set_title('Execution Time: ALL vs AFR', fontsize=14)
axs[0, 0].set_ylabel('Time (seconds)', fontsize=12)
axs[0, 0].set_xticks(x)
axs[0, 0].set_xticklabels(x_labels, rotation=45)
axs[0, 0].legend(fontsize=10, ncol=2)
axs[0, 0].grid(True)

# --- Plot 2: Speedup ---
axs[0, 1].plot(x, df['Speedup'], marker='D', color='purple', linewidth=2, markersize=8)
axs[0, 1].set_title('Global Speedup', fontsize=14)
axs[0, 1].set_ylabel('Speedup Factor', fontsize=12)
axs[0, 1].set_xticks(x)
axs[0, 1].set_xticklabels(x_labels, rotation=45)
axs[0, 1].grid(True)
# Add value labels on top of points
for i, v in enumerate(df['Speedup']):
    axs[0, 1].text(i, v + 0.05, str(v), ha='center', fontsize=9)

# --- Plot 3: Global Cost ---
bars = axs[1, 0].bar(x, df['Cost'], color='mediumseagreen', alpha=0.8, edgecolor='black', width=0.6)
axs[1, 0].set_title('Global Cost Analysis', fontsize=14)
axs[1, 0].set_ylabel('Cost', fontsize=12)
axs[1, 0].set_xticks(x)
axs[1, 0].set_xticklabels(x_labels, rotation=45)
axs[1, 0].set_ylim(0, df['Cost'].max() * 1.1)
axs[1, 0].grid(axis='y')

# --- Plot 4: Overhead (Residuals) ---
# Grouped bar chart for Residuals
width = 0.35
axs[1, 1].bar(x - width/2, df['R_all'], width, label='$R_{all}$', color='tab:blue', alpha=0.7, edgecolor='black')
axs[1, 1].bar(x + width/2, df['R_afr'], width, label='$R_{afr}$', color='tab:orange', alpha=0.7, edgecolor='black')

axs[1, 1].set_title('Task Overheads (Residuals)', fontsize=14)
axs[1, 1].set_ylabel('Overhead Time', fontsize=12)
axs[1, 1].set_xticks(x)
axs[1, 1].set_xticklabels(x_labels, rotation=45)
axs[1, 1].legend()
axs[1, 1].grid(axis='y')

# Common X-axis label
for ax in axs.flat:
    ax.set_xlabel('Configuration ($s_{all}, s_{afr}$)', fontsize=11)

plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.savefig('plots/task_performance_analysis.png', dpi=300)
plt.show()
