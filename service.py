"""GitAgent Runner — локальный HTTP-сервис для работы с репозиториями в Windows-ВМ.

Запуск:
    uvicorn service:app --host 0.0.0.0 --port 9000
    python service.py

Запросы принимаются только с 127.0.0.1 и 10.0.2.2 (адрес хоста при пробросе портов
VirtualBox). Остальные клиенты получают 403.
"""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

REPOS_DIR = Path(os.getenv("RUNNER_REPOS_DIR", r"C:\runner\repos"))
ALLOWED_HOSTS = {"127.0.0.1", "10.0.2.2"}

NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
URL_RE = re.compile(r"^(https?://|git@)[^\s]+$")

MANIFESTS = (
    "package.json",
    "requirements.txt",
    "pyproject.toml",
    "Dockerfile",
    "Makefile",
    "setup.py",
)

DEFAULT_TIMEOUTS = {"clone": 300.0, "install": 900.0, "run": 300.0, "wait_seconds": 5.0}
MAX_TIMEOUT = 3600.0
MAX_WAIT_SECONDS = 30.0
LOG_LINES = 200
STORE_LINES = 2000
OUTPUT_LINES = 120
OUTPUT_CHARS = 20000

CREATE_NO_WINDOW = 0x08000000
CREATE_NEW_PROCESS_GROUP = 0x00000200

app = FastAPI(title="GitAgent Runner", version="0.1.0")


class ActionRequest(BaseModel):
    """Тело запроса: {"action": "...", "params": {...}}."""

    action: str
    params: dict[str, Any] = Field(default_factory=dict)


class ActionError(Exception):
    """Ожидаемая ошибка действия: текст попадает в поля error и explanation."""


def reply(ok: bool, output: Any = "", explanation: str = "", error: str | None = None) -> dict:
    """Единый формат ответа сервиса."""

    return {"ok": ok, "output": output, "explanation": explanation, "error": error}


@app.middleware("http")
async def local_only(request: Request, call_next):
    """Пропускает только 127.0.0.1 и 10.0.2.2."""

    client_host = request.client.host if request.client else ""
    if client_host not in ALLOWED_HOSTS:
        return JSONResponse(
            status_code=403,
            content=reply(
                False,
                explanation=(
                    "Сервис принимает запросы только с 127.0.0.1 и 10.0.2.2, "
                    "поэтому запрос отклонён."
                ),
                error=f"Адрес {client_host or 'unknown'} не разрешён.",
            ),
        )
    return await call_next(request)


def repo_path(name: Any) -> Path:
    """Проверяет имя репозитория и возвращает путь внутри REPOS_DIR."""

    if not isinstance(name, str) or not NAME_RE.match(name):
        raise ActionError(
            "Некорректное имя репозитория: допустимы латиница, цифры, точка, "
            "дефис и подчёркивание (до 64 символов)."
        )

    base = REPOS_DIR.resolve()
    path = (base / name).resolve()
    if path.parent != base:
        raise ActionError("Имя репозитория выводит за пределы папки репозиториев.")

    return path


def parse_timeout(params: dict, key: str, default: float) -> float:
    """Читает таймаут из params и ограничивает сверху."""

    raw = params.get(key)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ActionError(f"Параметр {key} должен быть числом секунд.") from exc
    if value <= 0:
        raise ActionError(f"Параметр {key} должен быть больше нуля.")
    return min(value, MAX_TIMEOUT)


def trim_output(text: str, lines: int = OUTPUT_LINES, chars: int = OUTPUT_CHARS) -> str:
    """Оставляет последние непустые строки и ограничивает длину."""

    cleaned = (text or "").replace("\r\n", "\n").strip()
    parts = [line for line in cleaned.split("\n") if line.strip()]
    hidden = len(parts) - lines
    if hidden > 0:
        parts = [f"… ({hidden} строк скрыто)"] + parts[-lines:]
    result = "\n".join(parts)
    if len(result) > chars:
        result = result[-chars:]
    return result


def kill_tree(pid: int) -> None:
    """Останавливает процесс вместе с дочерними (npm/node, pip и т.п.)."""

    try:
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            creationflags=CREATE_NO_WINDOW,
            timeout=30,
        )
    except (subprocess.SubprocessError, OSError):
        pass


def run_process(
    args: list[str] | str,
    cwd: Path,
    timeout: float,
    shell: bool = False,
    env: dict[str, str] | None = None,
) -> dict:
    """Выполняет команду с таймаутом и возвращает код, вывод и флаг таймаута."""

    process = subprocess.Popen(
        args,
        cwd=str(cwd),
        shell=shell,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        creationflags=CREATE_NO_WINDOW,
    )

    try:
        output, _ = process.communicate(timeout=timeout)
        timed_out = False
    except subprocess.TimeoutExpired:
        kill_tree(process.pid)
        try:
            output, _ = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            output = ""
        timed_out = True

    return {
        "code": process.returncode,
        "output": trim_output(output or ""),
        "timed_out": timed_out,
    }


class Job:
    """Фоновый процесс, запущенный действием run."""

    def __init__(
        self,
        pid: int,
        process: subprocess.Popen,
        path: Path,
        command: str,
        timeout: float,
    ) -> None:
        self.pid = pid
        self.process = process
        self.path = path
        self.command = command
        self.timeout = timeout
        self.lines: deque[str] = deque(maxlen=STORE_LINES)
        self.lock = threading.Lock()
        self.finished = threading.Event()
        self.timed_out = False
        self.stopped = False
        self.started_at = time.time()
        self.timer: threading.Timer | None = None

    def read_output(self) -> None:
        try:
            if self.process.stdout is not None:
                for line in self.process.stdout:
                    with self.lock:
                        self.lines.append(line.rstrip("\n"))
        finally:
            self.process.wait()
            self.finished.set()
            timer = self.timer
            if timer is not None:
                timer.cancel()

    def snapshot(self, limit: int = LOG_LINES) -> list[str]:
        with self.lock:
            return list(self.lines)[-limit:]

    def kill_by_timeout(self) -> None:
        if self.finished.is_set():
            return
        self.timed_out = True
        kill_tree(self.pid)

    def stop(self) -> None:
        if self.finished.is_set():
            return
        self.stopped = True
        kill_tree(self.pid)

    def status(self) -> str:
        if not self.finished.is_set():
            return "работает"
        if self.timed_out:
            return "остановлен по таймауту"
        if self.stopped:
            return "остановлен вручную"
        return f"завершён, код {self.process.returncode}"


JOBS: dict[int, Job] = {}
JOBS_LOCK = threading.Lock()


def action_clone(params: dict) -> dict:
    """git clone в C:\\runner\\repos\\<name> с таймаутом."""

    url = params.get("url")
    if not isinstance(url, str) or not url.strip():
        raise ActionError("Не передан url репозитория.")
    url = url.strip()
    if not URL_RE.match(url):
        raise ActionError("Нужен http(s)- или git@-адрес репозитория.")

    destination = repo_path(params.get("name"))
    timeout = parse_timeout(params, "timeout", DEFAULT_TIMEOUTS["clone"])

    if destination.exists() and any(destination.iterdir()):
        raise ActionError(f"Папка {destination} уже существует и не пуста.")

    REPOS_DIR.mkdir(parents=True, exist_ok=True)

    git = shutil.which("git")
    if git is None:
        raise ActionError("git не найден в PATH внутри ВМ.")

    result = run_process(
        [git, "clone", "--depth", "1", url, str(destination)],
        REPOS_DIR,
        timeout,
        env={**os.environ.copy(), "GIT_TERMINAL_PROMPT": "0", "GIT_PAGER": "cat"},
    )

    if result["timed_out"]:
        return reply(
            False,
            result["output"],
            f"Клонирование не уложилось в {timeout:.0f} с, поэтому процесс остановлен.",
            f"Таймаут git clone ({timeout:.0f} с).",
        )
    if result["code"] != 0:
        return reply(
            False,
            result["output"],
            "git clone завершился с ошибкой — скорее всего, адрес недоступен или репозиторий закрыт.",
            f"git clone завершился с кодом {result['code']}.",
        )

    return reply(
        True,
        result["output"] or f"Склонировано в {destination}",
        f"Репозиторий склонирован в {destination} — только последний коммит, без истории.",
        None,
    )


def action_detect(params: dict) -> dict:
    """Файлы верхнего уровня и найденные манифесты."""

    path = repo_path(params.get("name"))
    if not path.is_dir():
        raise ActionError(f"Папка {path} не найдена — сначала выполните clone.")

    entries = sorted(path.iterdir(), key=lambda entry: entry.name.lower())
    files = [entry.name + ("/" if entry.is_dir() else "") for entry in entries]
    manifests = [name for name in MANIFESTS if (path / name).is_file()]

    output = {"path": str(path), "files": files, "manifests": manifests}

    if manifests:
        explanation = (
            f"В корне {len(files)} элементов, найдены манифесты: {', '.join(manifests)} — "
            "по ним можно ставить зависимости."
        )
    else:
        explanation = (
            f"В корне {len(files)} элементов, манифестов не найдено — "
            "автоматическая установка не сработает."
        )

    return reply(True, output, explanation, None)


def action_install(params: dict) -> dict:
    """pip install -r requirements.txt или npm install с таймаутом."""

    path = repo_path(params.get("name"))
    if not path.is_dir():
        raise ActionError(f"Папка {path} не найдена — сначала выполните clone.")

    timeout = parse_timeout(params, "timeout", DEFAULT_TIMEOUTS["install"])

    if (path / "requirements.txt").is_file():
        command: list[str] = [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"]
        label = "pip install -r requirements.txt"
    elif (path / "package.json").is_file():
        npm = shutil.which("npm")
        if npm is None:
            raise ActionError("npm не найден в PATH внутри ВМ.")
        command = [npm, "install", "--no-audit", "--no-fund"]
        label = "npm install"
    else:
        raise ActionError(
            "В корне репозитория нет requirements.txt или package.json — устанавливать нечего."
        )

    result = run_process(command, path, timeout)

    if result["timed_out"]:
        return reply(
            False,
            result["output"],
            f"{label} не уложился в {timeout:.0f} с, поэтому установка прервана.",
            f"Таймаут {label} ({timeout:.0f} с).",
        )
    if result["code"] != 0:
        return reply(
            False,
            result["output"],
            f"{label} завершился с ошибкой, поэтому зависимости не установлены.",
            f"{label} завершился с кодом {result['code']}.",
        )

    return reply(
        True,
        result["output"],
        f"Зависимости установлены командой {label} в {path}.",
        None,
    )


def action_run(params: dict) -> dict:
    """Запускает команду в фоне и возвращает первые строки вывода и PID."""

    path = repo_path(params.get("name"))
    if not path.is_dir():
        raise ActionError(f"Папка {path} не найдена — сначала выполните clone.")

    command = params.get("command")
    if not isinstance(command, str) or not command.strip():
        raise ActionError("Не передана команда для запуска.")
    command = command.strip()
    if len(command) > 2000:
        raise ActionError("Команда слишком длинная: максимум 2000 символов.")

    timeout = parse_timeout(params, "timeout", DEFAULT_TIMEOUTS["run"])
    wait_seconds = min(
        parse_timeout(params, "wait_seconds", DEFAULT_TIMEOUTS["wait_seconds"]),
        MAX_WAIT_SECONDS,
    )

    process = subprocess.Popen(
        command,
        cwd=str(path),
        shell=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={
            **os.environ.copy(),
            "PYTHONUNBUFFERED": "1",
            "PYTHONIOENCODING": "utf-8",
        },
        creationflags=CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP,
    )

    job = Job(process.pid, process, path, command, timeout)
    with JOBS_LOCK:
        JOBS[job.pid] = job

    threading.Thread(target=job.read_output, daemon=True).start()

    timer = threading.Timer(timeout, job.kill_by_timeout)
    timer.daemon = True
    job.timer = timer
    timer.start()

    deadline = time.time() + wait_seconds
    while time.time() < deadline and not job.finished.is_set() and not job.snapshot(limit=1):
        time.sleep(0.1)

    preview = job.snapshot(limit=20)
    output = f"PID: {job.pid}\nстатус: {job.status()}\n\n" + (
        "\n".join(preview) if preview else "(вывода пока нет)"
    )

    return reply(
        True,
        output,
        f"Команда «{command}» запущена в фоне (PID {job.pid}); выше её первые строки вывода. "
        f"Через {timeout:.0f} с процесс будет остановлен автоматически.",
        None,
    )


def job_by_pid(params: dict) -> Job:
    """Находит процесс, запущенный этим сервисом."""

    try:
        pid = int(params.get("pid"))
    except (TypeError, ValueError) as exc:
        raise ActionError("Параметр pid должен быть числом.") from exc

    with JOBS_LOCK:
        job = JOBS.get(pid)

    if job is None:
        raise ActionError(
            f"Процесс {pid} не найден среди запущенных сервисом — останавливать его не буду."
        )

    return job


def action_stop(params: dict) -> dict:
    """Останавливает фоновый процесс по PID."""

    job = job_by_pid(params)

    if job.finished.is_set():
        return reply(True, f"PID: {job.pid}", "Процесс уже завершён, останавливать нечего.", None)

    job.stop()
    job.finished.wait(timeout=10)

    return reply(
        True,
        f"PID: {job.pid}\nстатус: {job.status()}",
        f"Процесс {job.pid} остановлен вместе с дочерними процессами.",
        None,
    )


def action_logs(params: dict) -> dict:
    """Последние 200 строк вывода процесса."""

    job = job_by_pid(params)
    lines = job.snapshot(LOG_LINES)
    output = f"PID: {job.pid}\nстатус: {job.status()}\n\n" + (
        "\n".join(lines) if lines else "(вывода пока нет)"
    )

    return reply(
        True,
        output,
        f"Показаны последние {len(lines)} строк вывода процесса {job.pid}.",
        None,
    )


def remove_readonly(func: Callable[..., Any], target: str, _exc_info: tuple) -> None:
    """Снимает атрибут «только для чтения» и повторяет операцию (файлы git)."""

    os.chmod(target, stat.S_IWRITE | stat.S_IREAD)
    func(target)


def jobs_for_path(path: Path) -> list[Job]:
    """Процессы, запущенные сервисом из указанной папки или её подпапок."""

    with JOBS_LOCK:
        jobs = list(JOBS.values())

    found: list[Job] = []
    for job in jobs:
        try:
            job_path = job.path.resolve()
        except OSError:
            job_path = job.path
        if job_path == path or path in job_path.parents:
            found.append(job)

    return found


def action_delete(params: dict) -> dict:
    """Удаляет папку репозитория вместе с содержимым."""

    path = repo_path(params.get("name"))

    base = REPOS_DIR.resolve()
    if path == base or base not in path.parents:
        raise ActionError("Удалять можно только папки внутри каталога репозиториев.")

    if not path.exists():
        return reply(
            False,
            "",
            f"Папка {path} не найдена — возможно, её уже удалили.",
            f"Папка {path} не существует.",
        )

    if not path.is_dir():
        raise ActionError(f"{path} — не папка, удалять её не буду.")

    running = [job for job in jobs_for_path(path) if not job.finished.is_set()]
    for job in running:
        job.stop()
    for job in running:
        job.finished.wait(timeout=10)

    try:
        shutil.rmtree(path, onerror=remove_readonly)
    except OSError as exc:
        return reply(
            False,
            "",
            f"Папку {path} удалить не удалось — возможно, файлы заняты другим процессом. "
            "Подробности в поле error.",
            f"{type(exc).__name__}: {exc}",
        )

    stopped = ", ".join(str(job.pid) for job in running)
    explanation = f"Папка {path} удалена вместе с содержимым."
    if running:
        explanation += (
            " Перед удалением остановлены запущенные сервисом процессы: "
            f"{stopped} (чужие процессы не затрагивались)."
        )

    output = f"Удалено: {path}"
    if running:
        output += f"\nОстановлены процессы сервиса: {stopped}"

    return reply(True, output, explanation, None)


ACTIONS: dict[str, Callable[[dict], dict]] = {
    "clone": action_clone,
    "detect": action_detect,
    "install": action_install,
    "run": action_run,
    "stop": action_stop,
    "logs": action_logs,
    "delete": action_delete,
}


@app.post("/")
def handle_action(request: ActionRequest) -> dict:
    """Единая точка входа: действие выбирается по полю action."""

    action = (request.action or "").strip().lower()
    handler = ACTIONS.get(action)

    if handler is None:
        return reply(
            False,
            "",
            f"Действие «{action}» не поддерживается. Доступны: {', '.join(sorted(ACTIONS))}.",
            f"Неизвестное действие: {action}",
        )

    try:
        return handler(request.params)
    except ActionError as exc:
        return reply(False, "", f"Не удалось выполнить «{action}»: {exc}", str(exc))
    except Exception as exc:  # noqa: BLE001 - наружу отдаём тип и текст, без стека
        return reply(
            False,
            "",
            f"Внутренняя ошибка при выполнении «{action}» — подробности в поле error.",
            f"{type(exc).__name__}: {exc}",
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=9000)
