# CPU-only Llama 3 (8B-Instruct, GGUF) + Power Telemetry (Windows)
A lightweight experimental framework for measuring how the order of forced token interventions affect language model output probabilties and entropy.
## Description
This project implements a framework for measuring how forced tokens affect the output distributions of language models. It compares two execution orders (A &rarr; B and B &rarr; A) by probing token probabilities and entropy before and after interventions. The framework record per-run statistics and supports reproducible, order seperated analysis. Collected results are logged to support statistical analysis and reporting.
## Getting Started
### Dependencies
* Windows 11
* Python 3.9+
* Local GGUF language model compatible with llama-cpp-python
#### Python Libraries:
* llama-cpp-python
* huggingface_hub
### Installation
1. Clone the repository:
   ```
   git clone
   ```
