import logging
import re
import json
import asyncio
import textwrap  # 用于代码缩进嵌套，解决沙箱作用域问题
from typing import Dict, Any, List
from core.llm.AI_client import AIClient
from core.generation.scaffold import STChartScaffold

logger = logging.getLogger(__name__)


class CodeGenerator:
    """
    代码生成器 (V5.2 并发切片 & 沙箱作用域安全版)
    利用 asyncio 并发生成，使用嵌套闭包彻底解决 exec() 沙箱的 NameError 问题。
    （保留了原生的组件返回契约，不干预时间轴等非渲染组件）
    """

    def __init__(self, llm_client: AIClient):
        self.llm = llm_client
        self.scaffold = STChartScaffold()

    def _clean_markdown(self, text: str) -> str:
        if not text:
            return ""
        pattern = r"```(?:python)?\s*(.*?)```"
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        text = text.strip()
        text = re.sub(r"^```(python)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
        return text.strip()

    def _build_context_str(self, summaries: List[Dict[str, Any]]) -> str:
        context_str = ""
        for s in summaries:
            var_name = s.get('variable_name')
            col_stats = s.get('column_stats') or s.get('basic_stats', {}).get('column_stats', {})
            sem_analysis = s.get('semantic_analysis', {})
            col_meta = sem_analysis.get('column_metadata', {})

            col_desc_list = [f"{col}({info.get('dtype', 'unknown')})" for col, info in
                             col_stats.items()] if col_stats else ["(无列信息)"]

            semantic_hints = {col: meta.get('semantic_tag') for col, meta in col_meta.items() if
                              isinstance(meta, dict) and meta.get('semantic_tag')}

            context_str += f"- 变量 `{var_name}`:\n"
            context_str += f"  - 原始列名(区分大小写!): {', '.join(col_desc_list[:50])}\n"
            if semantic_hints:
                context_str += f"  - 关键语义: {json.dumps(semantic_hints, ensure_ascii=False)}\n"
            context_str += "\n"
        return context_str

    async def _generate_single_component(self, comp: Any, query: str, context_str: str, available_vars: List[str],
                                         time_bounds_hint: str, frame_format_hint: str, interaction_hint: str) -> Dict[
        str, str]:
        """并发 worker：为单一组件生成专属代码函数"""
        is_dict = isinstance(comp, dict)
        c_id = comp.get('id') if is_dict else getattr(comp, 'id', 'unknown')
        c_type = comp.get('type') if is_dict else getattr(comp, 'type', 'unknown')
        c_title = comp.get('title') if is_dict else getattr(comp, 'title', 'unknown')
        c_type_str = str(c_type).split('.')[-1].lower()

        # 处理 Python 函数名（破折号转下划线）
        safe_func_name = f"get_{c_id.replace('-', '_')}"

        c_conf = comp.get('chart_config', {}) if is_dict else getattr(comp, 'chart_config', {})
        m_conf = comp.get('map_config', []) if is_dict else getattr(comp, 'map_config', [])

        config_hint = ""
        if c_conf:
            ctype = c_conf.get('chart_type') if isinstance(c_conf, dict) else getattr(c_conf, 'chart_type', '')
            config_hint = f"Chart Type: {ctype}"

        is_anim = False
        if m_conf and isinstance(m_conf, list) and len(m_conf) > 0:
            first_layer = m_conf[0]
            is_anim = first_layer.get('is_animated') if isinstance(first_layer, dict) else getattr(first_layer,
                                                                                                   'is_animated', False)
            if is_anim: config_hint += f" [ANIMATED]"

        comp_desc = f"Component ID: `{c_id}`, Type: {c_type_str}, Title: {c_title}, Config: {config_hint}"

        # 核心切片：将单一组件传给 Scaffold
        system_prompt = self.scaffold.get_system_prompt(context_str, [comp])

        user_prompt = f"""
        User Query: "{query}"

        === TEMPORAL CONSTRAINTS ===
        Timeline range: {time_bounds_hint}
        Frame format: `{frame_format_hint}`

        === CRITICAL STANDARDS ===
        1. Only use variables: {json.dumps(available_vars)}.
        2. DO NOT hallucinate animation if not marked [ANIMATED].
        3. Filter data to stay within: {time_bounds_hint}.

        === YOUR SPECIFIC TASK ===
        You MUST ONLY write the code for this specific component:
        {comp_desc}

        Output format requirement (CRITICAL):
        ```python
        def {safe_func_name}(data_context):
            # Your specific logic here
            return final_result # return fig or df or dict
        ```
        """

        logger.info(f"  -> Concurrently generating code for: {c_id}")
        raw_response = await self.llm.chat_async([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ], json_mode=False)

        code_snippet = self._clean_markdown(raw_response)

        return {
            "id": c_id,
            "func_name": safe_func_name,
            "code": code_snippet
        }

    async def generate_dashboard_code(
            self,
            query: str,
            summaries: List[Dict[str, Any]],
            component_plans: List[Any],
            interaction_hint: str = ""
    ) -> str:

        context_str = self._build_context_str(summaries)
        available_vars = [s.get('variable_name') for s in summaries]

        frame_format_hint = "%H:00"
        time_bounds_hint = "No specific bounds"
        target_components = []

        if component_plans:
            for comp in component_plans:
                is_dict = isinstance(comp, dict)
                c_type = comp.get('type') if is_dict else getattr(comp, 'type', 'unknown')
                c_type_str = str(c_type).split('.')[-1].lower()

                if c_type_str == 'timeline_controller':
                    t_conf = comp.get('timeline_config', {}) if is_dict else getattr(comp, 'timeline_config', {})
                    if t_conf:
                        start = t_conf.get('start_time') if isinstance(t_conf, dict) else getattr(t_conf, 'start_time',
                                                                                                  '')
                        end = t_conf.get('end_time') if isinstance(t_conf, dict) else getattr(t_conf, 'end_time', '')
                        fmt = t_conf.get('frame_format') if isinstance(t_conf, dict) else getattr(t_conf,
                                                                                                  'frame_format', '')
                        if start and end: time_bounds_hint = f"FROM {start} TO {end}"
                        if fmt: frame_format_hint = fmt
                else:
                    target_components.append(comp)

        logger.info(f"Firing up {len(target_components)} parallel generation tasks...")

        tasks = [
            self._generate_single_component(
                comp, query, context_str, available_vars,
                time_bounds_hint, frame_format_hint, interaction_hint
            )
            for comp in target_components
        ]

        generated_results = await asyncio.gather(*tasks)

        # ---------------------------------------------------------
        # [核心修复 1]: 代码安全组装 - 使用闭包嵌套彻底解决沙箱 NameError
        # ---------------------------------------------------------
        master_script = "import pandas as pd\nimport geopandas as gpd\nimport plotly.express as px\nimport numpy as np\nimport json\n\n"
        master_script += "def get_dashboard_data(data_context):\n"
        master_script += "    dashboard_results = {}\n\n"

        # 遍历并发生成的所有函数，利用 textwrap 将其缩进后强塞入主函数内 (闭包)
        for res in generated_results:
            func_code = res['code']
            # 将生成的完整 def get_xxx(data_context): 函数块缩进 4 个空格
            indented_code = textwrap.indent(func_code, '    ')
            master_script += indented_code + "\n\n"

        # 组装安全执行块
        master_script += "    # ================= Execution Block =================\n"
        for res in generated_results:
            comp_id = res['id']
            func_name = res['func_name']
            master_script += f"    try:\n"
            master_script += f"        dashboard_results['{comp_id}'] = {func_name}(data_context)\n"
            master_script += f"    except Exception as e:\n"
            master_script += f"        print(f'Error generating {comp_id}: {{str(e)}}')\n"
            master_script += f"        dashboard_results['{comp_id}'] = None\n\n"

        master_script += "    return dashboard_results\n"

        logger.info("Parallel assembly complete. Returning unified execution script with sandbox isolation.")
        return master_script

    async def fix_code(self, original_code: str, error_trace: str, summaries: List[Dict[str, Any]],
                       component_plans: List[Any] = None) -> str:
        context_str = self._build_context_str(summaries)
        available_vars = [s.get('variable_name') for s in summaries]

        component_plans = component_plans or []
        base_prompt = self.scaffold.get_system_prompt(context_str, component_plans)

        fix_prompt = f"""
        CODE EXECUTION FAILED. 

        === ERROR TRACEBACK ===
        {error_trace}

        === ORIGINAL CODE ===
        {original_code}

        === 🚨 DIAGNOSTIC CHECKLIST 🚨 ===
        1. **Column Names**: Case sensitivity.
        2. **Variable Keys**: Only {json.dumps(available_vars)} exist.
        3. **Join Error**: Cast to string `.astype(str)` before merge.
        4. **CRS Error**: Ensure EPSG:4326 for maps.

        Return the complete FIXED code block.
        """

        logger.warning("Attempting self-healing fix for STV code...")
        raw_response = await self.llm.chat_async([
            {"role": "system", "content": base_prompt},
            {"role": "user", "content": fix_prompt}
        ], json_mode=False)

        return self._clean_markdown(raw_response)