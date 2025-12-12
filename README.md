# CPU-only Llama 3 (8B-Instruct, GGUF) + Power Telemetry (Windows)
A lightweight experimental framework for measuring how the order of forced token interventions affect language model output probabilties and entropy.
## Description
This project implements a framework for measuring how forced tokens affect the output distributions of language models. It compares two execution orders (A &rarr; B and B &rarr; A) by probing token probabilities and entropy before and after interventions. The framework record per-run statistics and supports reproducible, order seperated analysis. Collected results are logged to support statistical analysis and reporting.
## Getting Started
### Dependencies
* Windows 11
* Python 3.9+
* Local GGUF language model compatible with llama-cpp-python
  
Python Libraries:
* llama-cpp-python
* huggingface_hub
### Installation
1. Clone the repository:
   ```
   git clone https://github.com/grxam/Joules-Per-Bit
   cd Joules-Per-Bit
   ```
2. Create and activate a virtual environment:
   ```
   python -m venv .venv
   . .\.venv\Scripts\Activate.ps1
   ```
3. Upgrade pip:
   ```
   python -m pip install --upgrade pip
   ```
4. Install dependencies (CPU wheels)
   ```
   pip install -r requirements.txt --extra-index-url https://abetlen.github.io/llama-cpp-python/whl/cpu
   ```
### Model Download
1. Authenticate with Hugging Face and accept the model license
   ```
   hf auth login
   ```
2. Download model:
   ```
   hf download bartowski/Meta-Llama-3-8B-Instruct-GGUF Meta-Llama-3-8B-Instruct-Q4_K_M.gguf --local-dir models
   ```
   After completion, the model should be located at:
   ```
   models/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf
   ```
### Executing Program
1. Activate the virtual environment:
   ```
   . .\.venv\Scripts\Activate.ps1
   ```
2. Set required environment variables
   ```
   $env:LLAMA_MODEL_PATH="C:\path\to\models\Meta-Llama-3-8B-Instruct-Q4_K_M.gguf"
   $env:LLAMA_OUT_DIR="logs"
   ```
3. Run the experiment:
   ```
   python experiment_protocol.py --run-id 001 --mode A2B
   ```

#### Execution modes
* ```A2B``` - Run A &rarr; B protocol
* ```B2A``` - Run B &rarr; A protocol
* ```BOTH``` - Run both protocols
Output summaries are written to the specified output directory

### Power Measurement (Intel Power Gadget)
Power measurments were collected using Intel Power Gadget on Windows
1. Download and install [Intel Power Gadget](https://web.archive.org/web/20230325112308/https://www.intel.com/content/www/us/en/developer/articles/tool/power-gadget.html)
2. Verify installation in powershell:
   ```
   & "C:\Program Files\Intel\Power Gadget 3.6\Powerlog3.0.exe" -?
   ```
3. Run experiment with Intel Power Gadget:
   ```
   & "C:\Program Files\Intel\Power Gadget 3.6\PowerLog3.0.exe" -resolution 50 -file logs\run_001_A2B.csv -cmd "python experiment_protocol.py --run-id 001 --mode A2B"
   ```
### Idle Power Baseline

To isolate model-related energy usage, an idle CPU power baseline is measured and subtracted from each experimental run.

The idle baseline should be recorded once per session under similar system conditions
(same power plan, plugged in, minimal background activity).

Run the following command to collect an idle baseline:
```
& "C:\Program Files\Intel\Power Gadget 3.6\PowerLog3.0.exe" -resolution 50 -file logs\idle.csv -cmd "python -c ""import time; time.sleep(30)"""
```
This produces: ```logs/idle.csv```
Idle power is subtracted during aggregation to compute net power and net energy.

### Aggregating Results

After running experiments, all per-run summaries and power logs can be aggregated into a single results table.

The aggregation step:
* reads per-run summary CSV files
* reads Intel Power Gadget power logs
* subtracts idle power
* computes net energy and net average power

Run:
```
python aggregate_results.py
```
This produces ```logs/aggregate_results.csv```
The aggregated file contains one row per run and includes entropy metrics, power usage, and idle-subtracted energy values.


## License
This project is licensed under the MIT license - see the LICENSE file for details
