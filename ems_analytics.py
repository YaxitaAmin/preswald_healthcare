from preswald import connect, get_df, query, table, text, plotly
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots



# MODULE 1 - USING NYC EMS DATA TO SEE PERFORMANCE OF DIFFERENT EMS SERVICES AROUND NYC 1M DATASET DOWNLOADED FROM NYC OPEN DATA PORTAL
# AFTER LEARNING DATASET FROM DATA FINDINGS PY FILE NEEDED PREPROCESSING
connect()
raw_ems_data = get_df("nycems.csv")

# BACKUP THE ORIGINAL DATASET FOR SAFETY
original_dataset = raw_ems_data.copy()
working_dataset = raw_ems_data.copy()

# CONVERT STRING RESPONSE TIMES TO NUMERIC VALUES FOR CALCULATIONS
working_dataset['incident_response_seconds_qy'] = pd.to_numeric(working_dataset['incident_response_seconds_qy'], errors='coerce')
working_dataset['dispatch_response_seconds_qy'] = pd.to_numeric(working_dataset['dispatch_response_seconds_qy'], errors='coerce') 
clean_dataset = working_dataset.dropna(subset=['dispatch_response_seconds_qy', 'borough']).copy()




# MODULE 2 - QUERY FOR DATA MANIPULATION
# TO SEE BEST AND WORST PERFORMING BOROUGHS AND GET INSIGHTS FROM RESPONSE TIMES
text("# NYC EMS RESPONSE TIME ANALYSIS")
text("*Comprehensive performance dashboard for emergency medical services*")

# QUERY TO GET FAST EMERGENCY RESPONSES UNDER 5 MINUTES
fast_query = "SELECT * FROM nycems WHERE dispatch_response_seconds_qy <= 300"
speedy_calls = query(fast_query, "nycems")

# QUERY TO CHECK VOLUME OF CALLS BY BOROUGH WITH MINIMUM THRESHOLD
vol_query = "SELECT borough, COUNT(*) as call_volume FROM nycems GROUP BY borough HAVING COUNT(*) > 1000 ORDER BY call_volume DESC"
borough_volumes = query(vol_query, "nycems")

# QUERY TO IDENTIFY DELAYED RESPONSES OVER 8 MINUTES
slow_query = "SELECT * FROM nycems WHERE dispatch_response_seconds_qy > 480"
delayed_calls = query(slow_query, "nycems")

# CALCULATE COMPREHENSIVE STATISTICS FOR EACH BOROUGH
borough_stats = (clean_dataset.groupby('borough')
                .agg({
                    'dispatch_response_seconds_qy': ['mean', 'median', 'std', 'count'],
                    'incident_response_seconds_qy': ['mean', 'median']
                })
                .round(2))

borough_stats.columns = ['avg_dispatch_sec', 'median_dispatch_sec', 'std_dispatch', 'total_calls', 'avg_incident_sec', 'median_incident']
borough_stats = borough_stats.reset_index()

# CALCULATE PERFORMANCE COMPLIANCE RATES FOR INDUSTRY BENCHMARKS
performance_metrics = (clean_dataset.groupby('borough')
                      .apply(lambda x: pd.Series({
                          'meets_5min': (x['dispatch_response_seconds_qy'] <= 300).mean() * 100,
                          'meets_8min': (x['dispatch_response_seconds_qy'] <= 480).mean() * 100,
                          'extreme_delays': (x['dispatch_response_seconds_qy'] > 600).sum()
                      }), include_groups=False)
                      .round(2)
                      .reset_index())

master_data = borough_stats.merge(performance_metrics, on='borough')

# CREATE PERFORMANCE CATEGORIES FOR RESPONSE TIME ANALYSIS
clean_dataset['perf_bucket'] = pd.cut(clean_dataset['dispatch_response_seconds_qy'], 
                                     bins=[0, 300, 480, 600, np.inf], 
                                     labels=['Excellent (<5min)', 'Good (5-8min)', 'Poor (8-10min)', 'Critical (>10min)'])

bucket_counts = clean_dataset['perf_bucket'].value_counts().reset_index()
bucket_counts.columns = ['perf_bucket', 'incidents']

# CALCULATE CITYWIDE SYSTEM PERFORMANCE METRICS
total_calls = len(clean_dataset)
system_avg_min = clean_dataset['dispatch_response_seconds_qy'].mean() / 60
overall_8min_rate = (clean_dataset['dispatch_response_seconds_qy'] <= 480).mean() * 100
extreme_delays = (clean_dataset['dispatch_response_seconds_qy'] > 600).sum()

# CREATE EXECUTIVE SUMMARY DASHBOARD
kpi_summary = pd.DataFrame([
    ['Total Emergency Calls', f"{total_calls:,}"],
    ['Average Response Time', f"{system_avg_min:.1f} minutes"],
    ['8-Minute Target Rate', f"{overall_8min_rate:.1f}%"],
    ['Extreme Delays (>10 min)', f"{extreme_delays:,}"],
    ['System Status', 'Below Target' if overall_8min_rate < 90 else 'Meeting Standards']
], columns=['Metric', 'Value'])

best_borough = master_data.loc[master_data['meets_8min'].idxmax()]
worst_borough = master_data.loc[master_data['meets_8min'].idxmin()]
busiest_borough = master_data.loc[master_data['total_calls'].idxmax()]

key_insights = pd.DataFrame([
    ['Top Performer', f"{best_borough['borough']} ({best_borough['meets_8min']:.1f}% compliance)"],
    ['Improvement Needed', f"{worst_borough['borough']} ({worst_borough['meets_8min']:.1f}% compliance)"],
    ['Highest Volume', f"{busiest_borough['borough']} ({busiest_borough['total_calls']:,} calls)"],
    ['Boroughs Under 80%', f"{(master_data['meets_8min'] < 80).sum()} require attention"],
    ['Recommendation', 'Resource reallocation needed' if overall_8min_rate < 85 else 'Maintain current operations']
], columns=['Category', 'Finding'])




# MODULE 3 - EXECUTIVE DASHBOARD WITH CLEAR VISUALIZATIONS
# DISPLAY KEY PERFORMANCE INDICATORS FOR DECISION MAKERS
text("## EXECUTIVE OVERVIEW")
table(kpi_summary)

# CREATE BOROUGH COMPLIANCE CHART WITH PROPER AXIS LABELS
sorted_data = master_data.sort_values('meets_8min', ascending=False)
fig_main = px.bar(sorted_data,
                 x='borough', 
                 y='meets_8min',
                 color='meets_8min',
                 title='Borough Performance: 8-Minute Response Target',
                 labels={'meets_8min': 'Compliance Rate (%)', 'borough': 'NYC Borough'},
                 color_continuous_scale='RdYlGn',
                 text='meets_8min')

fig_main.add_hline(y=90, line_dash="dash", line_color="red", line_width=3, 
                  annotation_text="Target: 90%")
fig_main.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
fig_main.update_layout(height=450, showlegend=False)
fig_main.update_xaxes(title_text="NYC Borough")
fig_main.update_yaxes(title_text="8-Minute Compliance Rate (%)")
plotly(fig_main)

table(key_insights)




# MODULE 4 - DETAILED PERFORMANCE BREAKDOWN
# COMPREHENSIVE BOROUGH ANALYSIS WITH STATISTICAL MEASURES
text("## DETAILED BOROUGH METRICS")
table(master_data.sort_values('meets_8min', ascending=False))

# RESPONSE TIME DISTRIBUTION ANALYSIS WITH CLEAR AXES
clean_times = clean_dataset[clean_dataset['dispatch_response_seconds_qy'] <= 1800]
response_minutes = clean_times['dispatch_response_seconds_qy'] / 60

fig_dist = px.histogram(x=response_minutes,
                       nbins=40,
                       title='NYC EMS Response Time Distribution (Filtered)', 
                       labels={'x': 'Response Time (Minutes)', 'y': 'Number of Emergency Calls'},
                       color_discrete_sequence=['#2E86AB'])

fig_dist.add_vline(x=5, line_dash="dot", line_color="green", line_width=2, 
                  annotation_text="5min")
fig_dist.add_vline(x=8, line_dash="dash", line_color="orange", line_width=2, 
                  annotation_text="8min") 
fig_dist.add_vline(x=10, line_dash="solid", line_color="red", line_width=2, 
                  annotation_text="10min")
fig_dist.update_layout(height=450, bargap=0.05, xaxis_range=[0, 25])
fig_dist.update_xaxes(title_text="Response Time (Minutes)")
fig_dist.update_yaxes(title_text="Number of Emergency Calls")
plotly(fig_dist)

# CALL VOLUME VS PERFORMANCE CORRELATION ANALYSIS
fig_scatter = px.scatter(master_data,
                        x='total_calls',
                        y='meets_8min', 
                        size='avg_dispatch_sec',
                        color='borough',
                        title='Call Volume vs Performance Analysis',
                        labels={'total_calls': 'Total Emergency Calls', 'meets_8min': '8-Min Compliance (%)'},
                        size_max=25,
                        hover_data=['avg_dispatch_sec'])

fig_scatter.add_hline(y=90, line_dash="dash", line_color="#d62728", line_width=2)
fig_scatter.add_hline(y=80, line_dash="dot", line_color="#ff7f0e", line_width=2)
fig_scatter.update_layout(height=450)
fig_scatter.update_xaxes(title_text="Total Emergency Calls")
fig_scatter.update_yaxes(title_text="8-Minute Compliance Rate (%)")
plotly(fig_scatter)




# MODULE 5 - BOROUGH COMPARISON AND PERFORMANCE CATEGORIES
# SIDE BY SIDE COMPARISON OF ALL BOROUGHS WITH KEY METRICS
text("## COMPARATIVE ANALYSIS BY BOROUGH")

comparison_metrics = master_data[['borough', 'avg_dispatch_sec', 'meets_5min', 'meets_8min', 'total_calls', 'extreme_delays']]
comparison_metrics['avg_minutes'] = (comparison_metrics['avg_dispatch_sec'] / 60).round(1)
comparison_metrics = comparison_metrics.drop('avg_dispatch_sec', axis=1)
table(comparison_metrics.sort_values('meets_8min', ascending=False))

# STACKED BAR CHART SHOWING PERFORMANCE DISTRIBUTION
perf_by_borough = pd.crosstab(clean_dataset['borough'], clean_dataset['perf_bucket'], normalize='index') * 100
perf_by_borough = perf_by_borough.round(1).reset_index()

fig_stacked = px.bar(perf_by_borough.melt(id_vars=['borough'], var_name='perf_bucket', value_name='percentage'),
                    x='borough',
                    y='percentage',
                    color='perf_bucket', 
                    title='Performance Distribution Across Boroughs',
                    labels={'percentage': 'Percentage of Calls (%)', 'borough': 'NYC Borough'},
                    color_discrete_map={
                        'Excellent (<5min)': '#2ca02c',
                        'Good (5-8min)': '#1f77b4', 
                        'Poor (8-10min)': '#ff7f0e',
                        'Critical (>10min)': '#d62728'
                    })
fig_stacked.update_layout(height=450, barmode='stack')
fig_stacked.update_xaxes(title_text="NYC Borough")
fig_stacked.update_yaxes(title_text="Percentage of Emergency Calls (%)")
plotly(fig_stacked)

# BOX PLOT FOR RESPONSE TIME VARIABILITY ANALYSIS
filtered_for_box = clean_dataset[clean_dataset['dispatch_response_seconds_qy'] <= 1200].copy()

fig_variability = px.box(filtered_for_box,
                        x='borough',
                        y='dispatch_response_seconds_qy',
                        title='Response Time Consistency by Borough (Outliers Removed)',
                        labels={'dispatch_response_seconds_qy': 'Response Time (Seconds)', 'borough': 'NYC Borough'},
                        color='borough')

fig_variability.add_hline(y=300, line_dash="dot", line_color="green", line_width=2, 
                         annotation_text="5min", annotation_position="left")
fig_variability.add_hline(y=480, line_dash="dash", line_color="orange", line_width=2,
                         annotation_text="8min", annotation_position="left")
fig_variability.add_hline(y=600, line_dash="solid", line_color="red", line_width=2,
                         annotation_text="10min", annotation_position="left")
fig_variability.update_layout(height=450, showlegend=False,
                              xaxis_title="NYC Borough",
                              yaxis_title="Response Time (Seconds)")
plotly(fig_variability)




# MODULE 6 - ADVANCED ANALYTICS AND STATISTICAL INSIGHTS
# PERCENTILE ANALYSIS FOR DEEPER UNDERSTANDING OF PERFORMANCE
text("## ADVANCED PERFORMANCE INSIGHTS")

percentile_data = clean_dataset.groupby('borough')['dispatch_response_seconds_qy'].quantile([0.25, 0.5, 0.75, 0.9, 0.95]).unstack()
percentile_data.columns = ['p25', 'p50_median', 'p75', 'p90', 'p95'] 
percentile_data = percentile_data.reset_index()
percentile_minutes = percentile_data.copy()
for col in ['p25', 'p50_median', 'p75', 'p90', 'p95']:
    percentile_minutes[col] = (percentile_minutes[col] / 60).round(1)

table(percentile_minutes)

# MULTI DIMENSIONAL RADAR CHART FOR COMPREHENSIVE COMPARISON
fig_radar = go.Figure()

radar_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
for idx, borough_name in enumerate(master_data['borough']):
    borough_row = master_data[master_data['borough'] == borough_name].iloc[0]
    
    speed_score = 100 - ((borough_row['avg_dispatch_sec'] - master_data['avg_dispatch_sec'].min()) / 
                        (master_data['avg_dispatch_sec'].max() - master_data['avg_dispatch_sec'].min()) * 100)
    
    reliability_score = 100 - (borough_row['extreme_delays'] / master_data['extreme_delays'].max() * 100) if master_data['extreme_delays'].max() > 0 else 100
    
    fig_radar.add_trace(go.Scatterpolar(
        r=[borough_row['meets_8min'], 
           speed_score,
           borough_row['meets_5min'],
           reliability_score],
        theta=['8-Min Compliance', 'Response Speed', '5-Min Excellence', 'Reliability Score'],
        fill='toself',
        name=borough_name,
        line_color=radar_colors[idx % len(radar_colors)]
    ))

fig_radar.update_layout(
    polar=dict(radialaxis=dict(visible=True, range=[0, 100], title="Performance Score")),
    title="Borough Performance Radar Comparison",
    height=550
)
plotly(fig_radar)

# CORRELATION MATRIX FOR IDENTIFYING RELATIONSHIPS BETWEEN METRICS
corr_matrix = master_data[['avg_dispatch_sec', 'total_calls', 'meets_5min', 'meets_8min', 'extreme_delays']].corr()
fig_corr = px.imshow(corr_matrix,
                    text_auto=True,
                    aspect="auto", 
                    title="Performance Metrics Correlation Analysis",
                    color_continuous_scale='RdBu_r',
                    zmin=-1, zmax=1,
                    labels=dict(x="Performance Metrics", y="Performance Metrics", color="Correlation"))
fig_corr.update_layout(height=450,
                       xaxis_title="Performance Metrics",
                       yaxis_title="Performance Metrics")
plotly(fig_corr)

# DISPLAY EXAMPLES OF FAST RESPONSE INCIDENTS
if speedy_calls is not None and len(speedy_calls) > 0:
    fast_examples = speedy_calls.head(12)[['borough', 'final_call_type', 'dispatch_response_seconds_qy']].copy()
    fast_examples['response_min'] = (fast_examples['dispatch_response_seconds_qy'] / 60).round(1)
    table(fast_examples)

# SHOW BOROUGH CALL VOLUMES FROM SQL QUERY
if borough_volumes is not None and len(borough_volumes) > 0:
    table(borough_volumes)

# TIME TREND ANALYSIS IF DATE INFORMATION AVAILABLE
date_columns = [col for col in clean_dataset.columns if 'date' in col.lower() or 'time' in col.lower()]
if date_columns:
    date_col = date_columns[0]
    trend_data = clean_dataset.copy()
    trend_data[date_col] = pd.to_datetime(trend_data[date_col], errors='coerce')
    
    if not trend_data[date_col].isna().all():
        daily_trends = (trend_data.groupby(trend_data[date_col].dt.date)
                       .agg({'dispatch_response_seconds_qy': 'mean'})
                       .reset_index())
        daily_trends.columns = ['date', 'avg_response_sec']
        
        fig_trend = px.line(daily_trends,
                           x='date', 
                           y='avg_response_sec',
                           title='Daily Response Time Trends',
                           labels={'avg_response_sec': 'Average Response Time (Seconds)', 'date': 'Date'})
        fig_trend.add_hline(y=480, line_dash="dash", line_color="#d62728", line_width=2)
        fig_trend.update_layout(height=400,
                                xaxis_title="Date",
                                yaxis_title="Average Response Time (Seconds)")
        plotly(fig_trend)