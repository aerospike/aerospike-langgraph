# [Aerospike_Checkpoint_Langgraph]

<!-- Brief description of what your project does -->

Store LangGraph checkpoints in Aerospike using the provided `AerospikeSaver`. The repo includes a minimal Aerospike docker setup, examples, and pytest-based checks.

## Getting Started

### Development Setup

<!-- Add project-specific setup instructions here -->

```bash
# Clone the repository
git clone https://github.com/aerospike/aerospike-checkpoint-langgraph.git
cd aerospike-checkpoint-langgraph

# Install deps
#Python 3.10+
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
#aerospike client
docker compose up -d

```

## Project Structure

<!-- Describe your project structure here -->

Core implementation: `langgraph/checkpoint/aerospike/saver.py`.

```text
.
├── .github/
│   ├── workflows/                             # GitHub Actions workflows
│   └── dependabot.yml                         # Dependabot configuration
│
├── langgraph/checkpoint/aerospike/            # Aerospike checkpointer implementation
│   ├── __init__.py
│   └── saver.py                               # AerospikeSaver class
│
├── tests/                                     # Test using pytest
│   ├── conftest.py                            # Shared pytest fixtures
│   ├── test_customer_support_graph_live.py    # Airline customer support bot(requies Ollama and tools_download_db.py)
│   ├── test_debug_dump_checkpoint.py          # Takes a thread_id input and outputs the latest decoded checkpoint
│   └── ...                                     # Additional test files
│
├── docker-compose.yml                         # Local Aerospike environment
├── pyproject.toml                             # Build system + project metadata
├── requirements.txt                           # Python dependencies
├── README.md                                  # Project documentation
└── (config files: .gitignore, .actrc, commitlint, etc.)

```

## Test

Use in a graph:

```python
import aerospike
from langgraph.checkpoint.aerospike import AerospikeSaver

client = aerospike.client({"hosts": [("127.0.0.1", 3000)]}).connect()
saver = AerospikeSaver(client=client, namespace="test")

compiled = graph.compile(checkpointer=saver)  # graph is your LangGraph graph
compiled.invoke({"input": "hello"}, config={"configurable": {"thread_id": "demo"}})
```

- Run our tests:
  ```bash
  pytest
  ```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## Security

For information on reporting security vulnerabilities, please see [SECURITY.md](SECURITY.md).

## Repo Tooling

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on repo tooling and development environment setup.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

<!-- Add support information here -->

For questions or issues, please:

- Open an issue on GitHub
- Check existing documentation
- Contact the maintainers

---
