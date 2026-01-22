import matplotlib.pyplot as plt
import numpy as np

# --- 1. The Data ---
# s = Number of workers
s = np.array([1, 2, 4, 6, 8])

# Actual measured times from your table
t_actual = np.array([292, 150, 83, 65, 55])

# Naive Model (p=1) predictions
t_pred_naive = np.array([292, 146, 73, 49, 37])

# Fitted Model (p ≈ 0.96) predictions
# Using the values from your "Fitted Model" column
t_pred_fitted = np.array([292, 152, 82, 58, 47])

# Calculate Residuals (Absolute Error)
residuals_naive = np.abs(t_actual - t_pred_naive)
residuals_fitted = np.abs(t_actual - t_pred_fitted)

# --- 2. Plotting Setup ---
# Create a figure with two subplots side-by-side
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# --- Plot 1: Prediction Accuracy (Line Chart) ---
# Plot Actual Data
ax1.plot(s, t_actual, 'ko-', label='Actual ($T_{actual}$)', linewidth=2, markersize=8)

# Plot Naive Prediction
ax1.plot(s, t_pred_naive, 'r--s', label='Naive Pred ($p=1$)', linewidth=2, markersize=6, alpha=0.7)

# Plot Fitted Prediction
ax1.plot(s, t_pred_fitted, 'b--^', label='Fitted Pred ($p \\approx 0.96$)', linewidth=2, markersize=6, alpha=0.7)

# Formatting
ax1.set_title('Model Predictions vs Actual Execution Time', fontsize=14)
ax1.set_xlabel('Number of Workers ($s$)', fontsize=12)
ax1.set_ylabel('Execution Time (seconds)', fontsize=12)
ax1.legend(fontsize=11)
ax1.grid(True, linestyle='--', alpha=0.6)
ax1.set_xticks(s)  # Ensure x-axis shows exactly 1, 2, 4, 6, 8

# --- Plot 2: Residual Comparison (Bar Chart) ---
bar_width = 0.35
index = np.arange(len(s))

# Create bars
bars1 = ax2.bar(index - bar_width/2, residuals_naive, bar_width, 
                label='Naive $|R|$', color='salmon', alpha=0.8, edgecolor='black')
bars2 = ax2.bar(index + bar_width/2, residuals_fitted, bar_width, 
                label='Fitted $|R|$', color='cornflowerblue', alpha=0.8, edgecolor='black')

# Formatting
ax2.set_title('Absolute Residual Error', fontsize=14)
ax2.set_xlabel('Number of Workers ($s$)', fontsize=12)
ax2.set_ylabel('Residual Error (seconds)', fontsize=12)
ax2.set_xticks(index)
ax2.set_xticklabels(s)
ax2.legend(fontsize=11)
ax2.grid(True, axis='y', linestyle='--', alpha=0.6)

# Function to add number labels on top of bars
def add_labels(bars):
    for bar in bars:
        height = bar.get_height()
        if height > 0:  # Only label non-zero bars
            ax2.annotate(f'{int(height)}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=10, fontweight='bold')

add_labels(bars1)
add_labels(bars2)

# --- 3. Save and Show ---
plt.tight_layout()
plt.savefig('plots/comparison_plots1.png', dpi=300)
print("Plot saved as 'comparison_plots.png'")
plt.show()