# Cquirrel

Cquirrel is a demonstration system that turns SQL queries into Apache Flink jobs and visualizes the query execution workflow through a web UI.

Cquirrel：它可以把 SQL 查询转换成 Apache Flink，并通过 Web 界面展示查询执行流程。

## Repository Policy / 仓库数据政策

This repository is intended for GitHub source-code hosting. Production datasets, generated TPC-H streams, local runtime outputs, dependency folders, and build artifacts are not committed.

本仓库按 GitHub 源码仓库整理。生产数据集、生成后的 TPC-H 输入流、本地运行输出、依赖目录和构建产物都不应提交到 Git。

Ignored local artifacts include:

本地会被忽略的文件包括：

- `DemoTools/DataGenerator/*.tbl`
- `DemoTools/DataGenerator/input_data_*.csv`
- `gui/cquirrel_flask/cquirrel_app/resources/`
- `gui/cquirrel_react/node_modules/`
- `gui/cquirrel_react/build/`
- `codegen/target/`
- `.run/`, `.idea/`, `__pycache__/`, `.DS_Store`

If you need a dataset, generate it locally by following the steps below.

如果需要数据集，按下面步骤在本地重新生成。

## Directory Layout / 目录结构

- `DemoTools/DataGenerator`: TPC-H stream data generator. It consumes raw `.tbl` files and writes Cquirrel input streams.
- `codegen`: SQL-to-Flink code generator.
- `gui/cquirrel_flask`: Flask backend that controls code generation, Flink submission, and runtime settings.
- `gui/cquirrel_react`: React frontend.
- `scripts`: Helper scripts for local data generation and starting/stopping the demo stack.

- `DemoTools/DataGenerator`：TPC-H 输入流生成器，读取原始 `.tbl` 文件并生成 Cquirrel 输入流。
- `codegen`：SQL 到 Flink 程序的代码生成模块。
- `gui/cquirrel_flask`：Flask 后端，负责代码生成、Flink 提交和运行设置。
- `gui/cquirrel_react`：React 前端。
- `scripts`：本地生成数据、启动/停止演示系统的脚本。

## Requirements / 环境要求

The original demo was developed with the following versions:

原始演示环境使用以下版本：

- macOS Big Sur 11.5
- Google Chrome 91
- Python 3.8
- Java 8
- Scala 2.12
- Maven 3.6
- sbt 1.3
- yarn 1.22
- Apache Flink 1.11.2, Scala 2.12 build

Other versions may work, but Flink and Java version mismatches are the most common source of runtime failures.

其他版本可能也能运行，但 Flink 和 Java 版本不一致最容易导致运行失败。

## Prepare TPC-H Data / 准备 TPC-H 数据

Download the TPC-H tools from the TPC website and build `dbgen`.

从 TPC 官网下载 TPC-H Tools，并编译 `dbgen`。

```bash
cd /path/to/tpc-h_tools/dbgen
cp makefile.suite makefile
```

Edit `makefile` for your platform. A typical Linux/macOS setup is:

按你的平台修改 `makefile`。常见 Linux/macOS 配置如下：

```makefile
CC       = gcc
DATABASE = ORACLE
MACHINE  = LINUX
WORKLOAD = TPCH
```

Build and generate raw TPC-H table files:

编译并生成原始 TPC-H 表文件：

```bash
make
./dbgen -s 1 -vf
```

Copy the generated `.tbl` files into the generator directory:

把生成的 `.tbl` 文件复制到数据生成器目录：

```bash
cp *.tbl /path/to/Cquirrel-release/DemoTools/DataGenerator/
```

The `.tbl` files are local inputs only and are ignored by Git.

这些 `.tbl` 只是本地输入文件，已被 Git 忽略。

## Generate Cquirrel Input Streams / 生成 Cquirrel 输入流

Use the scale script for both insert-only and sliding-window streams.

使用通用脚本生成 insert-only 或 sliding-window 输入流。

```bash
cd /path/to/Cquirrel-release

# Small smoke dataset, insert-only stream
bash scripts/generate_scale_data.sh 0.001 insert_only

# 1% dataset, sliding-window stream
bash scripts/generate_scale_data.sh 0.01 sliding_windows
```

The generated file is copied to:

生成后的文件会复制到：

```text
gui/cquirrel_flask/cquirrel_app/resources/input_data_<scale>_<type>.csv
```

This directory is ignored by Git. For sliding windows, the default window size is `10%` of the generated lineitem rows. You can override it:

该目录已被 Git 忽略。对于 sliding windows，默认窗口大小为生成的 lineitem 行数的 `10%`，可以用环境变量修改：

```bash
CQUIRREL_SLIDING_WINDOW_RATIO=0.2 bash scripts/generate_scale_data.sh 0.01 sliding_windows
```

Common scale factors:

常用规模参数：

- `0.001`: smoke test / 冒烟测试
- `0.01`: 1%
- `0.15`: 15%
- `1`: full SF1 / 完整 SF1

## Build Local Artifacts / 构建本地产物

Install the Cquirrel core jar into your local Maven repository. If `cquirrel-core-1.0-SNAPSHOT.jar` is not present, download it from your release artifact location first.

把 Cquirrel core jar 安装到本地 Maven 仓库。如果当前目录没有 `cquirrel-core-1.0-SNAPSHOT.jar`，请先从你的发布产物位置下载。

```bash
mvn install:install-file \
  -Dfile=./cquirrel-core-1.0-SNAPSHOT.jar \
  -DgroupId=org.hkust \
  -DartifactId=cquirrel-core \
  -Dversion=1.0-SNAPSHOT \
  -Dpackaging=jar
```

Build the code generator and copy the jar for the Flask backend:

构建代码生成器，并把 jar 复制给 Flask 后端使用：

```bash
mvn -f codegen/pom.xml clean package
mkdir -p gui/cquirrel_flask/jar
cp codegen/target/codegen-1.0-SNAPSHOT.jar gui/cquirrel_flask/jar/codegen.jar
```

These generated jars are local build artifacts and should not be committed unless you intentionally publish release binaries.

这些生成的 jar 属于本地构建产物。除非明确要发布二进制 release，否则不要提交到 Git。

## Start Flink / 启动 Flink

Download Flink 1.11.2 for Scala 2.12:

下载 Scala 2.12 版本的 Flink 1.11.2：

```bash
curl -LO https://archive.apache.org/dist/flink/flink-1.11.2/flink-1.11.2-bin-scala_2.12.tgz
tar -xzf flink-1.11.2-bin-scala_2.12.tgz
cd flink-1.11.2
bin/start-cluster.sh
```

You can also set `FLINK_HOME` before starting the Cquirrel stack.

也可以在启动 Cquirrel 前设置 `FLINK_HOME`。

## Start Backend / 启动后端

```bash
cd gui/cquirrel_flask
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python cquirrel_gui.py
```

By default, the backend tries to discover a generated input file in `gui/cquirrel_flask/cquirrel_app/resources/`.

默认情况下，后端会在 `gui/cquirrel_flask/cquirrel_app/resources/` 中查找生成好的输入文件。

Useful environment variables:

常用环境变量：

- `CQUIRREL_INPUT_DATA_FILE`: absolute path to a CSV input stream.
- `CQUIRREL_DATASET`: dataset scale token, for example `smoke`, `1pct`, `15pct`, `all`, `0.1`, or `10pct`.
- `CQUIRREL_STREAMS_TYPE`: `insert_only` or `sliding_windows`.
- `FLINK_HOME`: path to the Flink installation.
- `JAVA_HOME`: path to Java 8.

- `CQUIRREL_INPUT_DATA_FILE`：CSV 输入流的绝对路径。
- `CQUIRREL_DATASET`：数据规模标识，例如 `smoke`、`1pct`、`15pct`、`all`、`0.1` 或 `10pct`。
- `CQUIRREL_STREAMS_TYPE`：`insert_only` 或 `sliding_windows`。
- `FLINK_HOME`：Flink 安装目录。
- `JAVA_HOME`：Java 8 安装目录。

Example:

示例：

```bash
CQUIRREL_DATASET=smoke CQUIRREL_STREAMS_TYPE=insert_only python cquirrel_gui.py
```

## Start Frontend / 启动前端

```bash
cd gui/cquirrel_react
yarn install
yarn start
```

Open `http://localhost:3000` in Chrome.

在 Chrome 中打开 `http://localhost:3000`。

## One-Command Local Stack / 一键本地启动

After Flink, dependencies, the backend jar, and a generated dataset are ready, you can use:

当 Flink、依赖、后端 jar 和生成好的数据集都准备好后，可以使用：

```bash
bash scripts/start_cquirrel_stack.sh
```

To select a dataset:

选择数据集：

```bash
CQUIRREL_DATASET=1pct CQUIRREL_STREAMS_TYPE=sliding_windows bash scripts/start_cquirrel_stack.sh
```

Stop the local stack:

停止本地服务：

```bash
bash scripts/stop_cquirrel_stack.sh
```

## GitHub Checklist / GitHub 上传检查

Before committing, run:

提交前执行：

```bash
git status --short
find DemoTools/DataGenerator -maxdepth 1 \( -name '*.tbl' -o -name 'input_data_*.csv' \) -print
find gui/cquirrel_flask/cquirrel_app/resources -type f -print 2>/dev/null
```

The last two commands should print nothing for a clean source-only commit.

对于纯源码提交，后两个命令应该没有输出。
