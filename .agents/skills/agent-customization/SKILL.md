---
name: agent-customization
scope: workspace
description: "Guides creation and maintenance of agent customization files (SKILL.md, .agent.md, .instructions.md) for this repository. Follow `brainstorming` first; the skill helps extract workflows from conversation history and produce a tested SKILL.md file."
---

# Agent Customization Skill — `agent-customization`

目的：把会话或团队已遵循的工作流抽象为可复用的 `SKILL.md`，并保存到仓库以便复用与审查。

先决条件：
- 在进行任何创作或变更前，先运行 `brainstorming` 技能并获得用户对设计的批准。
- 有访问仓库根目录的权限，且遵循仓库约定的文件位置（默认 `.agents/skills/`）。

输出：
- 一个 `SKILL.md` 文件，包含：步骤流程、决策点、质量准则、示例提示和维护说明。

步骤（逐条执行，按需循环）：
1. 探索会话与仓库上下文
   - 检查会话历史、相关 issue/PR、最近提交与现有技能文件
2. 提取工作流要素
   - 列出明确的步骤、条件分支、验收标准与完成检查点
3. 判断范围与受众
   - 选择 `workspace`（仓库范围）或 `personal`（个人助手）作用域
4. 草拟 `SKILL.md`
   - 包含：目标、触发条件、输入输出、步骤清单、分支逻辑、质量检查项、示例提示
5. 标识模糊点并提问
   - 一次一个问题，优先多选题，直到能把模糊点写清楚
6. 收集用户反馈并迭代
   - 更新草稿，重复 4-6 次直到用户批准
7. 保存并提交
   - 将文件保存到 `.agents/skills/<skill-name>/SKILL.md`（可在仓库中），并建议用户提交到 git
8. 提供示例与使用方法
   - 给出 3 个示例提示，展示如何调用该技能

决策点与分支：
- 如果会话描述模糊或范围过大：先请求把项目拆分为子任务；为每个子任务生成单独的 `SKILL.md`。
- 如果设计需要视觉说明：按 `brainstorming` 要求，单独发起“视觉伴侣”消息并征得用户同意。

质量准则（完成检查表）：
- 每个步骤均可通过“一次性问题”验证
- 至少列出 2 个可替代方案并给出推荐与理由
- 明确的完成准则（验收测试或文件保存位置）
- 无未解决的 `TBD`/`TODO` 占位符

示例提示（用户可直接使用）：
- "请基于刚才的对话，把我们的部署流程抽成一个 SKILL.md，重点标注回滚和健康检查的决策点。"
- "为 `feature X` 生成 agent-customization 风格的 SKILL.md，范围限定为后端变更和数据库迁移。"
- "我需要一个个人助手技能，提示我写 PR 描述：生成一版 `personal` 作用域的 SKILL.md。"

维护与元数据建议：
- 在文件头使用 YAML 元数据：`name`, `scope`, `description`, `version`, `lastUpdated`。
- 在仓库顶层维护 `README.md` 列表，指向所有 `.agents/skills/` 下的技能
- 定期（如每季度）审查技能以确保它们仍适用

常见问题与注意事项：
- 任何实现性行动（生成代码、编辑文件、运行脚本）在 `brainstorming` 通过之前都不应执行
- 每次迭代只问一个问题，倾向提供多项选择以加快决策

保存位置示例：
- `.agents/skills/agent-customization/SKILL.md`

---

结束语：在你确认是否希望把此技能作为 `workspace` 还是 `personal` 范围后，我可以：
- 更新 `scope` 字段并提交到仓库
- 生成 2-3 个具体示例提示供测试
- 帮你把此文件加入 git 并给出提交命令
