import matplotlib.pyplot as plt
import numpy as np

def create_plots(task_name, s_levels, t_pred_naive_map, t_pred_fitted_map, actuals_map, output_filename):
    # Prepare data arrays for plotting lines
    s_array = np.array(s_levels)
    t_naive_line = np.array([t_pred_naive_map[s] for s in s_levels])
    t_fitted_line = np.array([t_pred_fitted_map[s] for s in s_levels])
    
    # Calculate Residuals (Mean Absolute Error for each s)
    mae_naive = []
    mae_fitted = []
    
    # Prepare scatter data
    scatter_s = []
    scatter_t = []
    
    for s in s_levels:
        acts = actuals_map[s]
        # For scatter plot
        scatter_s.extend([s] * len(acts))
        scatter_t.extend(acts)
        
        # For residuals
        # Naive Residuals
        r_naive = [abs(a - t_pred_naive_map[s]) for a in acts]
        mae_naive.append(np.mean(r_naive))
        
        # Fitted Residuals
        r_fitted = [abs(a - t_pred_fitted_map[s]) for a in acts]
        mae_fitted.append(np.mean(r_fitted))
        
    mae_naive = np.array(mae_naive)
    mae_fitted = np.array(mae_fitted)

    # --- Plotting ---
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # --- Plot 1: Prediction Accuracy ---
    # Plot Actuals (Scatter)
    ax1.plot(scatter_s, scatter_t, 'ko', label='Actual ($T_{actual}$)', markersize=8, alpha=0.6)
    
    # Plot Naive Line
    ax1.plot(s_array, t_naive_line, 'r--s', label='Naive Pred ($p=1$)', linewidth=2, markersize=6, alpha=0.7)
    
    # Plot Fitted Line
    ax1.plot(s_array, t_fitted_line, 'b--^', label=f'Fitted Pred ($p_{{obs}}$)', linewidth=2, markersize=6, alpha=0.7)
    
    ax1.set_title(f'{task_name}: Model Predictions vs Actual Time', fontsize=14)
    ax1.set_xlabel('Number of Workers ($s$)', fontsize=12)
    ax1.set_ylabel('Execution Time (seconds)', fontsize=12)
    ax1.legend(fontsize=11)
    ax1.grid(True, linestyle='--', alpha=0.6)
    ax1.set_xticks(s_levels)
    
    # --- Plot 2: Residual Comparison ---
    bar_width = 0.35
    index = np.arange(len(s_levels))
    
    bars1 = ax2.bar(index - bar_width/2, mae_naive, bar_width, 
                    label='Naive Mean $|R|$', color='salmon', alpha=0.8, edgecolor='black')
    bars2 = ax2.bar(index + bar_width/2, mae_fitted, bar_width, 
                    label='Fitted Mean $|R|$', color='cornflowerblue', alpha=0.8, edgecolor='black')
    
    ax2.set_title(f'{task_name}: Mean Absolute Residual Error', fontsize=14)
    ax2.set_xlabel('Number of Workers ($s$)', fontsize=12)
    ax2.set_ylabel('Mean Residual Error (seconds)', fontsize=12)
    ax2.set_xticks(index)
    ax2.set_xticklabels(s_levels)
    ax2.legend(fontsize=11)
    ax2.grid(True, axis='y', linestyle='--', alpha=0.6)
    
    # Labels
    def add_labels(bars):
        for bar in bars:
            height = bar.get_height()
            if height > 0.1: 
                ax2.annotate(f'{height:.1f}',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    add_labels(bars1)
    add_labels(bars2)
    
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300)
    plt.close()

# --- Data Entry ---

# ALL Task Data
s_all_levels = [1, 2, 3, 4]
# From Table: Naive T_pred for ALL
# s=1: 101 (Baseline)
# s=2: 50
# s=3: 34
# s=4: 25
t_pred_naive_all = {1: 101, 2: 50, 3: 34, 4: 25}

# From Table: Fitted T_pred for ALL (p=0.61)
# s=1: 101 (Baseline)
# s=2: 70
# s=3: 60
# s=4: 55
t_pred_fitted_all = {1: 101, 2: 70, 3: 60, 4: 55}

# From Table: Actuals for ALL (group by s_all)
# s=1: 101
# s=2: 72, 76
# s=3: 60, 65, 67
# s=4: 54, 46, 50, 48
actuals_all = {
    1: [101],
    2: [72, 76],
    3: [60, 65, 67],
    4: [54, 46, 50, 48]
}

# AFR Task Data
s_afr_levels = [1, 2, 3, 4]
# From Table: Naive T_pred for AFR (Baseline 55)
# s=1: 55
# s=2: 28
# s=3: 18
# s=4: 14
t_pred_naive_afr = {1: 55, 2: 28, 3: 18, 4: 14}

# From Table: Fitted T_pred for AFR (p=0.58)
# s=1: 55
# s=2: 39
# s=3: 34
# s=4: 31
t_pred_fitted_afr = {1: 55, 2: 39, 3: 34, 4: 31}

# From Table: Actuals for AFR (group by s_afr)
# Rows:
# 1,1 -> s_afr=1, T=55
# 2,1 -> s_afr=1, T=64
# 2,2 -> s_afr=2, T=57
# 3,1 -> s_afr=1, T=66
# 3,2 -> s_afr=2, T=60
# 3,3 -> s_afr=3, T=50
# 4,1 -> s_afr=1, T=69
# 4,2 -> s_afr=2, T=50
# 4,3 -> s_afr=3, T=42
# 4,4 -> s_afr=4, T=37
actuals_afr = {
    1: [55, 64, 66, 69],
    2: [57, 60, 50],
    3: [50, 42],
    4: [37]
}

# Generate Plots
create_plots("ALL Task", s_all_levels, t_pred_naive_all, t_pred_fitted_all, actuals_all, "plots/all_task_plots.png")
create_plots("AFR Task", s_afr_levels, t_pred_naive_afr, t_pred_fitted_afr, actuals_afr, "plots/afr_task_plots.png")