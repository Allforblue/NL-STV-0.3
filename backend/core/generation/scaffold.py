from typing import List, Dict, Any
import json


class STChartScaffold:
    """
    Spatio-Temporal Chart Scaffold (V5.5 - Professional UI & Slicing)
    严格保留了防崩溃规则，并引入了专业 GIS 视觉标准（细边框、对数色阶、竖向图例、富文本悬浮框）。
    """

    def __init__(self):
        # ==========================================
        # 1. 全局基础规则 (任何组件生成都必须携带)
        # ==========================================
        self.global_rules = """
        [CRITICAL RULES - READ CAREFULLY]
        1. **NO DISK I/O**: `data_context` ALREADY contains loaded objects. 
           - ✅ Correct: `df = data_context['df_variable_name']`
           - ❌ Wrong: `gdf = gpd.read_file(...)`
        2. **VARIABLE NAMES**: 
           - USE EXACTLY the variable names provided in the Metadata Context.
           - DO NOT assume variable names. Always check `data_context.keys()` logic if unsure.
        3. **IMPORTS**: You MUST explicitly import ALL libraries: `import pandas as pd`, `import geopandas as gpd`, `import plotly.express as px`, `import numpy as np`, `import json`.
        4. **DATA CLEANING (Anti-Crash)**:
           - **Isolation**: Always use `.copy()` when fetching from `data_context` to prevent cross-component data pollution.
           - Before plotting, DROP NaNs: `df = df.dropna(subset=['col_x', 'col_y'])`.
           - For Bar/Line/Pie: FILTER out <=0 values if log scale or ratio is used.
        7. **RETURN FORMAT**: 
           - Function: `def get_dashboard_data(data_context):`.
           - Return a `dict` where keys are Component IDs and values are Figures/DataFrames.
        8. **INSIGHT DATA**: 
           - For 'insight' components, NEVER return a raw DataFrame. You must return a dict with the structure: {'summary': 'short_text', 'detail': 'long_text', 'evidence': df.to_dict()}.
        11. **NO INTERNAL CONTROLS (UI HYGIENE)**:
            - ❌ NEVER set `title=...` inside Plotly functions. UI handles titles externally.
            - ❌ For ANY animated component, you MUST hide internal Plotly controls: `fig.update_layout(updatemenus=[dict(visible=False)], sliders=[dict(visible=False)])`.
        13. **VISUAL STYLE & COLORBARS (CRITICAL UI HYGIENE)**: 
            - **Template**: Always use `template='plotly_white'` or `template='plotly_dark'` as requested.
            - **Continuous Scale**: For maps and heatmaps, use `color_continuous_scale='Viridis'`. Discrete: `px.colors.qualitative.Prism`.
            - ❌ NEVER use `orientation='h'` for any colorbar. It destroys the UI layout.
            - ✅ ALWAYS keep colorbars vertical, thin, and space-saving on the right side.
            - ✅ For maps, forcibly override colorbar style: `fig.update_layout(coloraxis_colorbar=dict(thickness=15, len=0.8, x=1.0, y=0.5, title=''))`
        14. **INTERACTION ANCHORS (IMPORTANT)**:
            - You MUST include the comment `#[INTERACTION_HOOK]` at the very beginning of the logic block for each component.
        15. **PLOTLY API COMPATIBILITY**: 
            - NEVER use `titleside` in `colorbar`. Use `title={'text': '...', 'side': 'top'}` instead.
            - DO NOT use `margin_t`, use `margin=dict(t=...)`.
        16. **JOIN TYPE ALIGNMENT**:
            - When merging tables, verify that the join keys are of the same data type. Convert to string using .astype(str) before merging.
        """

        # ==========================================
        # 2. 地图专属规则 (引入边界细化与多维回填)
        # ==========================================
        self.map_rules = """[MAP GEOMETRY STRICT RULES]
        5. **MAP GEOMETRY (Choropleth & Scatter)**: 
           - Mapbox only supports WGS84. Ensure `gdf = gdf.to_crs(epsg=4326)`.
           - For Choropleth: `Ensure gdf.reset_index(drop=True)` after any join or filter.
           - 🎨 **UI AESTHETICS (CRITICAL)**: Plotly's default choropleth borders are too thick. You MUST add `fig.update_traces(marker_line_width=0.2, marker_line_color='rgba(255,255,255,0.3)')` to make polygon borders thin and elegant.
        6. **LONG-TAIL DATA (CRITICAL)**: Taxi data is highly skewed. You MUST calculate a log score for colors: `df['color_score'] = np.log1p(df['count_column'])`. Use `color='color_score'` for drawing, but hide it in tooltips.
        9. **MAP TOOLTIPS & ENRICHMENT**:
           - ❌ NEVER show raw IDs (LocationID) or log scores in tooltips.
           - ✅ ALWAYS try to merge the `Borough` (district) column from the zone lookup table into your final dataframe.
           - ✅ ALWAYS use `labels={'Borough': '行政区', 'count': '订单量'}` to make tooltips professional.
           - ✅ Format numbers with thousands separators in `hover_data` (e.g., `'count': ':,.0f'`).
        10. **LARGE SCALE DATA (Performance & Memory Guard)**:
            - For Mapbox Scatter, if `len(df) > 50000`, you MUST use `df = df.sample(50000)`.
        """

        # ==========================================
        # 3. 时间轴动画专属规则 (保持极度稳定)
        # ==========================================
        self.animation_rules = """[DYNAMIC ANIMATION & STABILITY STRICT GUIDELINES]
        10b. **CRITICAL (MemoryError Fix)**: For animations, NEVER include the 'geometry' column in the DataFrame passed to `animation_frame`. Pass a separate static GeoJSON dict instead.
        17. **DYNAMIC ANIMATION & STABILITY (STRICT GUIDELINES)**:
            - **ID Type Sync (MANDATORY)**: Before creating GeoJSON, you MUST cast the ID column in the GDF to string: `gdf['ID'] = gdf['ID'].astype(str)`. Then `json.loads(gdf.to_json())`.
            - **Zero-Padding**: Ensure all IDs exist in every time frame (use a Cartesian product) and fill missing values with 0.
            - **Attribute Backfilling**: Merge descriptive names (e.g., 'Zone', 'Borough') back to the padded DataFrame for tooltips.
            - **Dual Sorting (MANDATORY)**: ALWAYS sort the final DataFrame by `[time_frame, id_col]`. If you only sort by time, the Plotly WebGL engine will freeze or glitch.
        18. **NO HARDCODED FILTERING (CRITICAL)**:
            - ❌ NEVER filter the dataframe to a specific hardcoded date like `df[df['date'] == '2025-01-01']` unless the user explicitly asks for exactly one single day. 
            - For dynamic animations, process the ENTIRE dataset provided in `data_context`. The timeline slider will naturally handle the temporal progression.
        19. **HOVER DATA PERSISTENCE (CRITICAL)**:
            - In animated maps, Plotly often drops hover information in subsequent frames. 
            - ❌ You MUST explicitly include the animation frame column in the `hover_data` parameter to ensure the tooltip updates.
        """

        # ==========================================
        # 4. 图表排版专属规则
        # ==========================================
        self.chart_rules = """[CHART SPECIFIC RULES]
        6. **BAR CHART LAYOUT**: 
           - For horizontal bars, construct a UNIQUE label to avoid stacking.
           - Layout: `fig.update_layout(margin=dict(l=150), yaxis=dict(automargin=True))`
        12. **TIME SERIES HANDLING**:
            - **Conversion**: Always use `df['time_col'] = pd.to_datetime(df['time_col'], errors='coerce')`.
            - **Aggregation**: Use `df.set_index('time_col').resample(time_bucket).size()` for trends. 
            - **Filling Gaps**: Use `.fillna(0)` to ensure lines connect properly in charts.
            - **Cyclic Patterns**: Use `df['time_col'].dt.hour` or `.dt.dayofweek` for periodic analysis.
        """

        # ==========================================
        # 5. 7 大 Recipe (已植入高级 UI 美学规范)
        # ==========================================
        self.recipes = {
            "choropleth_map": """[Recipe A: Professional Choropleth Map with Log Scaling]
        Target: "Spatial distribution with concentration"
        Code:
        ```python
        #[INTERACTION_HOOK]
        gdf_map = data_context['df_taxi_zones'].copy().to_crs(epsg=4326)
        df_stats = data_context['df_counts'].copy()

        # Step 0: Ensure ID is string for both mapping and GeoJSON
        gdf_map['LocationID'] = gdf_map['LocationID'].astype(str)
        df_stats['LocationID'] = df_stats['LocationID'].astype(str)

        # Step 1: Merge Data (Ensure Borough is included if available)
        gdf_map = gdf_map.merge(df_stats, on='LocationID', how='left')
        gdf_map['actual_count'] = gdf_map['order_count'].fillna(0)

        # Step 2: Log Scale for Coloring Skewed Data
        gdf_map['color_score'] = np.log1p(gdf_map['actual_count'])

        # Step 3: Professional Plotly Rendering
        fig = px.choropleth_mapbox(
            gdf_map, geojson=json.loads(gdf_map.to_json()), locations='LocationID',
            featureidkey="properties.LocationID",
            color='color_score', 
            hover_name='Zone', # Show Zone name as title in hover
            hover_data={
                'Borough': True,             # Show Borough
                'actual_count': ':,.0f',     # Format with thousands separator
                'color_score': False,        # Hide log score
                'LocationID': False          # Hide raw ID
            },
            labels={'Borough': '行政区', 'actual_count': '订单总量'},
            mapbox_style="carto-darkmatter", color_continuous_scale="Viridis", 
            template="plotly_dark", zoom=9.5, opacity=0.8
        )

        # Step 4: UI Aesthetics (Thin borders & Vertical Colorbar)
        fig.update_traces(marker_line_width=0.2, marker_line_color='rgba(255,255,255,0.3)')
        # 计算 4 个等分刻度
        vals = np.linspace(df_map['actual_count'].min(), df_map['actual_count'].max(), 4)
        tickvals = np.log1p(vals) # 对数空间映射
        ticktext = [f"{int(v):,}" for v in vals] # 真实数值显示
        fig.update_layout(
            coloraxis_colorbar=dict(thickness=15, len=0.8, x=1.0, y=0.5, title='订单数', tickvals=tickvals, 
                ticktext=ticktext)
        )
        ```""",
            "scatter_map": """[Recipe B: Scatter Mapbox]
        Code:
        ```python
        # [INTERACTION_HOOK]
        df = data_context['df_variable_name'].copy()
        if len(df) > 50000: 
            df = df.sample(50000, random_state=42)
        df = df.dropna(subset=['lat', 'lon'])

        fig = px.scatter_mapbox(
            df, lat='lat', lon='lon', color='val', size='val', 
            color_continuous_scale="Plasma", template="plotly_dark",
            mapbox_style="carto-darkmatter", size_max=15
        )
        fig.update_layout(coloraxis_colorbar=dict(thickness=15, len=0.8, x=1.0, y=0.5, title=''))
        ```""",
            "bar": """[Recipe C: Bar Chart Rankings]
        Code:
        ```python
        # [INTERACTION_HOOK]
        df = data_context['df_variable_name'].copy()
        df_agg = df.groupby('Category')['val'].sum().reset_index().sort_values('val', ascending=True).tail(10)

        fig = px.bar(
            df_agg, x='val', y='Category', orientation='h',
            color='Category', color_discrete_sequence=px.colors.qualitative.Prism,
            template="plotly_dark"
        )
        ```""",
            "pie": """[Recipe D: Smart Pie Chart]
        Code:
        ```python
        # [INTERACTION_HOOK]
        df = data_context['df_variable_name'].copy()
        df_pie = df['Borough'].value_counts().reset_index().head(8)

        fig = px.pie(
            df_pie, names='index', values='Borough', hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Prism,
            template="plotly_dark"
        )
        ```""",
            "line": """[Recipe E: Time-Series Trend Line Chart]
        Target: "Analyze trends over time"
        Code:
        ```python
        #[INTERACTION_HOOK]
        df = data_context['df_variable_name'].copy()
        df['time'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
        df = df.dropna(subset=['time'])

        df_trend = df.set_index('time').resample('1H').size().reset_index(name='count')
        df_trend = df_trend.fillna(0)

        fig = px.line(
            df_trend, x='time', y='count', template="plotly_dark"
        )
        fig.update_traces(mode='lines+markers', line=dict(width=3))
        ```""",
            "heatmap": """[Recipe F: Periodicity Analysis (Hour/Day Heatmap)]
        Code:
        ```python
        #[INTERACTION_HOOK]
        df = data_context['df_variable_name'].copy()
        df['hour'] = pd.to_datetime(df['time'], errors='coerce').dt.hour
        df_hour = df.groupby('hour').size().reset_index(name='count')
        fig = px.bar(
            df_hour, x='hour', y='count', template="plotly_dark"
        ) 
        ```""",
            "animated_map": """[Recipe G: Professional Dynamic Evolution Map]
        Target: "Smooth animation, forced string IDs, and dual sorting with rich tooltips"
        Code:
        ```python
        # [INTERACTION_HOOK]
        gdf_zones = data_context['df_zones'].copy().to_crs(epsg=4326)
        df_trips = data_context['df_trips'].copy()

        # 1. MANDATORY: Cast ID to STR BEFORE to_json (Ensures matching)
        gdf_zones['LocationID'] = gdf_zones['LocationID'].astype(str)
        geo_json = json.loads(gdf_zones.to_json())

        # 2. Extract Name Mapping (Backfill Zone AND Borough)
        name_columns = ['LocationID', 'Zone']
        if 'Borough' in gdf_zones.columns: name_columns.append('Borough')
        name_map = gdf_zones[name_columns].drop_duplicates()

        # 3. Time Grid & Zero-Padding
        df_trips['time_frame'] = pd.to_datetime(df_trips['time']).dt.strftime('%H:00')
        times = sorted(df_trips['time_frame'].unique())
        ids = gdf_zones['LocationID'].unique()

        grid = pd.MultiIndex.from_product([ids, times], names=['LocationID', 'time_frame']).to_frame(index=False)
        grid['LocationID'] = grid['LocationID'].astype(str)

        # 4. Aggregation & Backfilling
        df_agg = df_trips.groupby(['LocationID', 'time_frame']).size().reset_index(name='count')
        df_agg['LocationID'] = df_agg['LocationID'].astype(str)

        df_anim = pd.merge(grid, df_agg, on=['LocationID', 'time_frame'], how='left').fillna(0)
        df_anim = pd.merge(df_anim, name_map, on='LocationID', how='left') # Backfill properties

        # 5. Log Scale & MANDATORY DUAL SORT
        df_anim['color_score'] = np.log1p(df_anim['count'])
        df_anim = df_anim.sort_values(['time_frame', 'LocationID'])

        # 6. Professional Plot rendering
        fig = px.choropleth_mapbox(
            df_anim, geojson=geo_json, locations='LocationID', featureidkey="properties.LocationID",
            color='color_score', animation_frame='time_frame', animation_group='LocationID',
            hover_name='Zone',
            hover_data={
                'Borough': True if 'Borough' in df_anim.columns else False,
                'count': ':,.0f', 
                'time_frame': True,
                'color_score': False,
                'LocationID': False
            },
            labels={'Borough': '行政区', 'count': '订单总量', 'time_frame': '时间段'},
            mapbox_style="carto-darkmatter", zoom=9.5, opacity=0.8, template="plotly_dark",
            color_continuous_scale="Viridis"
        )

        # 7. UI Aesthetics: Thin borders, vertical colorbar, hide controls
        fig.update_traces(marker_line_width=0.2, marker_line_color='rgba(255,255,255,0.3)')
        # 计算全局最大值用于固定图例
        max_val = df_anim['count'].max()
        # 创建 4 个档位，例如 0, 1/3 max, 2/3 max, max
        vals = np.linspace(0, max_val, 4)
        tickvals = np.log1p(vals)
        ticktext = [f"{int(v):,}" for v in vals]
        fig.update_layout(
            updatemenus=[dict(visible=False)], sliders=[dict(visible=False)],
            coloraxis_colorbar=dict(
                thickness=15, len=0.8, x=1.0, y=0.5, 
                title='订单数', 
                tickvals=tickvals, 
                ticktext=ticktext
            )
        )
        ```"""
        }

    def get_system_prompt(self, context_str: str, component_plans: List[Any] = None) -> str:
        """
        根据规划组件动态组装 Prompt，完全无损保留规则。
        """
        if component_plans is None:
            component_plans = []

        needs_map = False
        needs_animation = False
        needs_chart = False

        active_recipes = []
        active_rules = [self.global_rules]

        for comp in component_plans:
            is_dict = isinstance(comp, dict)
            c_type = comp.get('type') if is_dict else getattr(comp, 'type', None)
            c_type_str = str(c_type).split('.')[-1].lower() if c_type else ""

            if c_type_str == "map":
                needs_map = True
                m_conf = comp.get('map_config', []) if is_dict else getattr(comp, 'map_config', [])

                # 判断是静态地图还是动态地图
                is_anim = False
                if m_conf and isinstance(m_conf, list) and len(m_conf) > 0:
                    first_layer = m_conf[0]
                    is_anim = first_layer.get('is_animated') if isinstance(first_layer, dict) else getattr(first_layer,
                                                                                                           'is_animated',
                                                                                                           False)

                if is_anim:
                    needs_animation = True
                    if "animated_map" not in active_recipes:
                        active_recipes.append("animated_map")
                else:
                    # 静态地图默认给 Choropleth (Recipe A) 和 Scatter (Recipe B) 供 LLM 自主选择
                    if "choropleth_map" not in active_recipes: active_recipes.append("choropleth_map")
                    if "scatter_map" not in active_recipes: active_recipes.append("scatter_map")

            elif c_type_str == "chart":
                needs_chart = True
                c_conf = comp.get('chart_config', {}) if is_dict else getattr(comp, 'chart_config', {})
                chart_type = c_conf.get('chart_type') if isinstance(c_conf, dict) else getattr(c_conf, 'chart_type',
                                                                                               'bar')
                chart_type_str = str(chart_type).split('.')[-1].lower()

                # 严格匹配 Enum 类型与 Recipe Key
                if chart_type_str in self.recipes and chart_type_str not in active_recipes:
                    active_recipes.append(chart_type_str)

        # 组装对应的规则模块
        if needs_map: active_rules.append(self.map_rules)
        if needs_animation: active_rules.append(self.animation_rules)
        if needs_chart: active_rules.append(self.chart_rules)

        # 兜底：如果完全没有组件或解析失败，退回全量输出（确保健壮性）
        if not active_recipes:
            active_recipes = list(self.recipes.keys())

        recipes_str = "\n".join([self.recipes[key] for key in active_recipes])
        rules_str = "\n".join(active_rules)

        prompt = f"""
        You are an Expert Python Spatio-Temporal Data Scientist.
        Your task is to complete the `get_dashboard_data(data_context)` function using `plotly.express`.

        === DATA METADATA (Context) ===
        {context_str}

        === EXPERT INSTRUCTIONS ===
        {rules_str}

        === RECIPES (The "Best Practice" Patterns) ===
        {recipes_str}

        === FINAL TASK ===
        1. Analyze User Query and components.
        2. Choose appropriate Recipe from above for each component.
        3. STRICTLY follow the EXPERT INSTRUCTIONS above.
        4. Return {{ 'comp_id': fig/df, ... }}.
        """
        return prompt