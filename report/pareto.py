import matplotlib.pyplot as plt
import pandas as pd

# 1. Define the data
data = {
    's': [1, 2, 4, 6, 8],
    'T_actual': [292, 150, 83, 65, 55],
    'Cost': [292, 300, 332, 390, 440]
}
df = pd.DataFrame(data)

# 2. Setup the plot
plt.figure(figsize=(8, 6))
plt.rcParams.update({'font.size': 12})

# 3. Plot the Pareto Frontier
# Connect points with a dashed line to show the trend/frontier
plt.plot(df['Cost'], df['T_actual'], linestyle='--', color='gray', zorder=1)
# Plot the actual data points
plt.scatter(df['Cost'], df['T_actual'], color='#1f77b4', s=100, zorder=2, label='Configurations')

# 4. Annotate each point with the thread count 's'
for i, row in df.iterrows():
    plt.annotate(f"s={int(row['s'])}", 
                 (row['Cost'], row['T_actual']),
                 xytext=(10, 10), textcoords='offset points',
                 arrowprops=dict(arrowstyle='->', color='black'))

# 5. Labels and styling
plt.title('Pareto Front: Cost vs. Time')
plt.xlabel('Cost')
plt.ylabel('Time ($T_{actual}$)')
plt.grid(True, linestyle='--', alpha=0.5)
plt.legend()

plt.tight_layout()
plt.savefig('plots/pareto_frontier.png', dpi=300)
plt.show()
