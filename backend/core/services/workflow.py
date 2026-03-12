import logging
import traceback
import json
import asyncio
import orjson
from typing import List, Dict, Any, Optional
from datetime import date, datetime

# --- 核心模块导入 ---
from core.llm.AI_client import AIClient
from core.profiler.semantic_analyzer import SemanticAnalyzer
from core.profiler.relation_mapper import RelationMapper
from core.profiler.interaction_mapper import InteractionMapper
from core.generation.dashboard_planner import DashboardPlanner
from core.generation.viz_generator import CodeGenerator
from core.generation.viz_editor import VizEditor
from core.execution.executor import CodeExecutor
from core.execution.insight_extractor import InsightExtractor

# --- 协议与 Schema 导入 ---
from core.schemas.dashboard import DashboardSchema, ComponentType
from core.schemas.interaction import InteractionPayload, InteractionTriggerType
from core.schemas.state import SessionStateSnapshot

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class AnalysisWorkflow:
    """
    全链路指挥官 (V2.5 时空组件保护版 / V3.0 AI智能决策与语义增强版)：
    1. [Performance] 优化 orjson 序列化管道，支持地理几何对象转换。
    2. [Stability] 强化异步画像分析，注入物理统计量指纹。
    3. [Decision] 引入大模型意图识别，自动判定 Edit/Generate 模式，支持强制干预。
    4. [Init] 支持数据上传后的系统级自动看板生成。
    5. [Semantic] 支持将 LLM 深度分析后的语义标签回传前端。
    """

    def __init__(self, llm_client: AIClient):
        self.llm = llm_client
        self.analyzer = SemanticAnalyzer(llm_client)
        self.relation_mapper = RelationMapper(llm_client)
        self.interaction_mapper = InteractionMapper(llm_client)
        self.planner = DashboardPlanner(llm_client)
        self.generator = CodeGenerator(llm_client)
        self.editor = VizEditor(llm_client)
        self.executor = CodeExecutor()
        self.insight_extractor = InsightExtractor(llm_client)

    async def _decide_workflow_mode(self, payload: InteractionPayload, last_state: dict) -> bool:
        """
        [新增] 智能决策引擎：判定本次交互是 Edit(编辑) 还是 Generate(重新生成)
        返回 True 表示 Edit Mode，返回 False 表示 Generate Mode
        """
        # 1. 绝对规则：UI 操作一定是 Edit (过滤/缩放/点击)
        if payload.trigger_type == InteractionTriggerType.UI_ACTION:
            logger.info(">>> [Mode Decision] 判定为 Edit: 触发源为 UI 交互")
            return True

        # 2. 绝对规则：如果没有任何历史代码，必须是 Generate
        if not last_state or not last_state.get("last_code"):
            logger.info(">>> [Mode Decision] 判定为 Generate: 无历史代码上下文")
            return False

        # 3. 尊重用户强制干预 (从 payload.force_mode 获取，兼容旧版 force_new)
        force_mode = getattr(payload, 'force_mode', 'auto')
        if force_mode == "edit":
            logger.info(">>> [Mode Decision] 判定为 Edit: 用户强制要求编辑模式")
            return True
        elif force_mode == "generate" or payload.force_new:
            logger.info(">>> [Mode Decision] 判定为 Generate: 用户强制要求生成模式")
            return False

        # 4. 初始化空问题：系统自动触发，走 Generate
        if payload.trigger_type == InteractionTriggerType.SYSTEM and not payload.query:
            logger.info(">>> [Mode Decision] 判定为 Generate: 系统自动初始化看板")
            return False

        # 5. AI 智能意图识别
        logger.info(">>> [Mode Decision] 启动 LLM 意图识别，判定用户的 Query 倾向...")
        prompt = f"""
        当前系统已经生成了一个数据可视化看板。用户输入了一个新的自然语言需求。
        请你判断用户是希望：
        1. "edit" (修改/增量): 在当前图表基础上微调（例如：改颜色、换图表、增减筛选、修改标题、在侧边加个图等）。
        2. "generate" (重建): 完全推翻当前分析方向，生成一个截然不同的新主题看板。

        用户的新需求: "{payload.query}"

        请严格输出 JSON 格式，包含字段 "mode"，值为 "edit" 或 "generate"。
        """
        try:
            decision = await self.llm.query_json_async(
                prompt=prompt,
                system_prompt="You are a strict JSON-output Intent Classifier."
            )
            mode = decision.get("mode", "edit").lower()
            logger.info(f">>> [Mode Decision] LLM 意图识别结果: {mode}")
            return mode == "edit"
        except Exception as e:
            logger.warning(f"意图识别失败，降级为 Edit 模式: {e}")
            return True

    async def execute_step(
            self,
            payload: InteractionPayload,
            data_summaries: List[Dict[str, Any]],
            data_context: Dict[str, Any],
            session_service: Any
    ) -> DashboardSchema:

        # === 0. 逻辑分流：历史回溯 ===
        if payload.trigger_type == InteractionTriggerType.BACKTRACK and payload.target_snapshot_id:
            logger.info(f">>> [Backtrack] 正在还原历史快照: {payload.target_snapshot_id}")
            snapshot = session_service.get_snapshot(payload.session_id, payload.target_snapshot_id)
            if snapshot:
                return snapshot.layout_data
            else:
                logger.error("快照不存在，降级为普通分析")

        # === 1. 数据增强与交互映射 (Profiling) ===

        # 1.1 并行执行语义分析 (此处会调用 LLM 补全列描述和语义标签)
        analysis_tasks = []
        task_indices = []

        for i, summary in enumerate(data_summaries):
            sem_analysis = summary.get("semantic_analysis", {})
            # 只有当缺少核心列元数据时才触发分析
            if not sem_analysis.get("column_metadata") and "file_info" in summary:
                logger.info(f">>>[Analysis] 调度异步语义画像: {summary['variable_name']}")
                fingerprint = summary.get("basic_stats", {})
                task = self.analyzer.analyze(summary["file_info"].get("path"), fingerprint)
                analysis_tasks.append(task)
                task_indices.append(i)
            else:
                logger.info(f">>> [Skip] 变量 {summary['variable_name']} 已有完整画像")

        if analysis_tasks:
            results = await asyncio.gather(*analysis_tasks)
            for idx, res in zip(task_indices, results):
                data_summaries[idx]["semantic_analysis"] = res.get("semantic_analysis", {})
                data_summaries[idx]["variable_name"] = res.get("variable_name", data_summaries[idx]["variable_name"])

        # ---[Eager Loading] 后台全量预加载 ---
        session_info = session_service.get_session(payload.session_id)
        if session_info and not session_info.get("is_full_data"):
            logger.info(">>> [Eager Load] 触发后台全量数据预加载 (Non-Blocking)...")

        # 1.2 交互锚点识别 (带缓存机制)
        session_state = session_service.get_session(payload.session_id)
        cached_anchors = session_state.get("cached_interaction_anchors")

        if not cached_anchors:
            logger.info(">>> [Interaction] 计算并缓存多数据集交互锚点...")
            interaction_anchors = await self.interaction_mapper.identify_interaction_anchors(data_summaries)
            session_state["cached_interaction_anchors"] = interaction_anchors
        else:
            logger.info(">>> [Cache] 命中交互锚点缓存")
            interaction_anchors = cached_anchors

        interaction_hint = self.interaction_mapper.get_planner_hints(interaction_anchors)

        try:
            # === 2. 逻辑决策：编辑模式 vs 生成模式 ===
            last_state = session_state.get("last_workflow_state") if session_state else None

            # [新增] 针对上传后的自动生成：如果是系统触发且无问题，注入自动探索 Prompt
            if payload.trigger_type == InteractionTriggerType.SYSTEM and not payload.query:
                logger.info(">>> [Auto-Dashboard] 检测到初始化请求，自动注入探索指令...")
                payload.query = "请详细分析数据的字段分布、地理特征和业务逻辑，并为我自动生成一个最具洞察力的初始看板。"

            # [修改] 使用 AI 与用户决策结合的模式判定逻辑
            is_edit_mode = await self._decide_workflow_mode(payload, last_state)

            current_code = ""
            dashboard_plan: DashboardSchema = None

            if is_edit_mode:
                # === 模式 A: 基于现有代码的增量编辑 (VizEditor) ===
                logger.info(f">>> [Edit Mode] 响应交互动作，正在修改代码逻辑...")
                dashboard_plan = DashboardSchema(**last_state["last_layout"])

                active_comp = next((c for c in dashboard_plan.components if c.id == payload.active_component_id), None)
                links = active_comp.links if active_comp else []

                current_code = await self.editor.edit_dashboard_code(
                    original_code=last_state["last_code"],
                    payload=payload,
                    summaries=data_summaries,
                    links=links
                )
            else:
                # === 模式 B: 从零规划并生成看板 (Planner + Generator) ===
                logger.info(">>>[Generate Mode] 正在规划全新时空看板...")
                dashboard_plan = await self.planner.plan_dashboard(
                    query=f"{payload.query}\n{interaction_hint}",
                    summaries=data_summaries
                )
                current_code = await self.generator.generate_dashboard_code(
                    query=payload.query,
                    summaries=data_summaries,
                    component_plans=dashboard_plan.components,
                    interaction_hint=interaction_hint
                )

            # 同步全局时间范围与视图状态
            if payload.time_range:
                dashboard_plan.global_time_range = payload.time_range
            if payload.view_state:
                dashboard_plan.initial_view_state.update(payload.view_state)

            # === 3. 代码执行 ===
            session_service.ensure_full_data_context(payload.session_id)
            full_session = session_service.get_session(payload.session_id)
            actual_data_context = full_session["data_context"]
            comp_ids = [c.id for c in dashboard_plan.components]

            exec_result = self.executor.execute_dashboard_logic(
                code_str=current_code,
                data_context=actual_data_context,
                component_ids=comp_ids
            )

            # 自愈机制
            if not exec_result.success:
                logger.warning(f"代码执行失败，启动 AI 自动纠错: {exec_result.error[:100]}...")
                current_code = await self.generator.fix_code(current_code, exec_result.error, data_summaries)
                exec_result = self.executor.execute_dashboard_logic(
                    current_code, actual_data_context, comp_ids
                )
                if not exec_result.success: raise Exception(f"代码修复失败: {exec_result.error}")

            # === 4. 结果装配与洞察提取 ===
            insight_card = await self.insight_extractor.generate_insights(
                query=payload.query or "交互更新分析",
                execution_stats=exec_result.global_insight_data,
                summaries=data_summaries
            )

            logger.info(">>> [Serialization] 正在进行数据序列化...")
            for component in dashboard_plan.components:
                if component.id in exec_result.results:
                    res = exec_result.results[component.id]
                    component.data_payload = self._sanitize_data_fast(res.data)

                    # 显式处理洞察组件
                    if component.type == ComponentType.INSIGHT:
                        raw_insight = exec_result.results[
                            component.id].data if component.id in exec_result.results else insight_card
                        clean_insight = self._sanitize_data_fast(raw_insight)
                        if isinstance(clean_insight, dict):
                            if 'Description' in clean_insight and 'detail' not in clean_insight:
                                clean_insight['detail'] = clean_insight.pop('Description')
                        component.insight_config = clean_insight
                        component.data_payload = clean_insight

            # === 5. 状态固化与快照存档 ===
            snapshot_id = session_service.save_snapshot(
                session_id=payload.session_id,
                query=payload.query or f"交互: {payload.active_component_id or '全局筛选'}",
                code=current_code,
                layout_data=dashboard_plan,
                summary=insight_card.summary
            )

            # [修改] metadata 注入更新后的语义画像，供前端获取列标签和业务语义
            dashboard_plan.metadata = {
                "last_code": current_code,
                "last_layout": dashboard_plan.model_dump(),
                "snapshot_id": snapshot_id,
                "enriched_summaries": self._sanitize_data_fast(data_summaries)  # <-- 透传语义分析结果
            }
            session_service.update_session_metadata(payload.session_id, dashboard_plan.metadata)

            return dashboard_plan

        except Exception as e:
            logger.error(f"Analysis Workflow Failed: {traceback.format_exc()}")
            raise e

    def _sanitize_data_fast(self, obj: Any) -> Any:
        """[极速序列化 V3.1 - 时空增强版]"""

        def deep_clean(o):
            if isinstance(o, (str, int, float, bool, type(None))):
                return o
            if isinstance(o, dict):
                return {str(k): deep_clean(v) for k, v in o.items()}
            if isinstance(o, (list, tuple, set)):
                return [deep_clean(i) for i in o]
            if isinstance(o, np.ndarray):
                return o.tolist()
            if isinstance(o, (np.integer, np.floating)):
                return o.item()
            if isinstance(o, (pd.Series, pd.Index)):
                return o.tolist()
            if isinstance(o, pd.DataFrame):
                return o.to_dict(orient='records')
            if hasattr(o, "__geo_interface__"):
                return o.__geo_interface__
            if hasattr(o, "to_dict") and hasattr(o, "layout") and hasattr(o, "data"):
                return deep_clean(o.to_dict())
            if hasattr(o, "model_dump"):
                return deep_clean(o.model_dump())
            if isinstance(o, (date, datetime, pd.Timestamp)):
                return o.isoformat()
            return str(o)

        try:
            clean_obj = deep_clean(obj)
            return orjson.loads(orjson.dumps(clean_obj, option=orjson.OPT_NON_STR_KEYS))
        except Exception as e:
            logger.error(f"Fast sanitization failed: {e}")
            return self._sanitize_data_legacy(obj)

    def _sanitize_data_legacy(self, obj: Any) -> Any:
        """兜底清洗逻辑"""
        if hasattr(obj, "to_dict"):
            try:
                return self._sanitize_data_legacy(obj.to_dict(orient='records'))
            except:
                return self._sanitize_data_legacy(obj.to_dict())
        if isinstance(obj, (np.ndarray, np.generic)):
            return obj.tolist() if isinstance(obj, np.ndarray) else obj.item()
        elif hasattr(obj, "to_plotly_json"):
            return self._sanitize_data_legacy(obj.to_plotly_json())
        elif isinstance(obj, dict):
            return {str(k): self._sanitize_data_legacy(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._sanitize_data_legacy(i) for i in obj]
        return str(obj)