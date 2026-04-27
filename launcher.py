import subprocess
import sys
from pathlib import Path


def launch(configs_dir: str, max_bots: int, venv_path: str = ".venv") -> None:
    """Lança um bot por par em sessões tmux separadas.

    Args:
        configs_dir: Pasta com os TOMLs dos pares.
        max_bots: Quantidade máxima de bots a lançar.
        venv_path: Caminho do virtualenv.
    """
    configs = sorted(Path(configs_dir).glob("*.toml"))

    if not configs:
        print("Nenhum TOML encontrado.")
        return

    configs = configs[:max_bots]

    for toml in configs:
        session_name = toml.stem
        python_bin = str(Path(venv_path) / "bin" / "python")
        cmd = f"{python_bin} main.py {toml}"

        subprocess.run(["tmux", "new-session", "-d", "-s", session_name, cmd])

        print(f"Bot lançado: {session_name} → {toml.name}")

    print(f"\n{len(configs)} bots em execução. Use 'tmux ls' para listar.")


if __name__ == "__main__":
    max_bots = int(sys.argv[1]) if len(sys.argv) > 1 else 3

    launch(configs_dir="usdm_futures/shared/data/symbol_configs", max_bots=max_bots)
