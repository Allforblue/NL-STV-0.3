### NL-STV 0.3系列版本
基于以下需求：
1. 理解数据，给出一些基础分析和可视化，等待用户问题
2. 用户问问题之后，能回答用户问题，相应给出可视化显示
3. 动态展示功能
4. 交互，地图上自主选择数据

#### 项目组织架构
暂未更新
```text
NL-STV-V0.2.1
├── backend/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── chat.py
│   │   ├── data.py
│   │   └── session.py
│   └── core/
│       ├── data_sandbox/
│       ├── execution/
│       │   ├── __init__.py
│       │   ├── executor.py
│       │   └── insight_extractor.py
│       ├── generation/
│       │   ├── __init__.py
│       │   ├── viz_generator.py
│       │   ├── dashboard_planner.py
│       │   ├── scaffold.py
│       │   ├── templates.py
│       │   └── viz_editor.py
│       ├── ingestion/
│       │   ├── __init__.py
│       │   ├── ingestion.py
│       │   └── loader_factory.py
│       ├── llm/
│       │   ├── __init__.py
│       │   └── AI_client.py
│       ├── profiler/
│       │   ├── __init__.py
│       │   ├── basic_stats.py
│       │   ├── interaction_mapper.py
│       │   ├── relation_mapper.py
│       │   └── semantic_analyzer.py
│       ├── schemas/
│       │   ├── __init__.py
│       │   ├── dashboard.py
│       │   ├── interaction.py
│       │   └── state.py
│       ├── services/
│       │   ├── __init__.py
│       │   ├── session_service.py
│       │   └── workflow.py
│       └── __init__.py
├── test/
├── __init__.py
├── app.py
├── main.py
└── pytest.ini
```