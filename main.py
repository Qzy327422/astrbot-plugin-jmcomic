"""
JMComic 禁漫搜索 - AstrBot 插件
/jm <关键词>   → 搜索本子
/jm <本子ID>   → 下载整部漫画并发送加密ZIP
"""
import os
import glob
import shutil
import base64
import uuid
import asyncio
import inspect
import gc
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from astrbot.api.message_components import Image, Nodes, Plain, Node
from astrbot.api import logger, AstrBotConfig
from astrbot.api.star import Context, Star, register
from astrbot.api.event import filter

try:
    import jmcomic
    from jmcomic import (
        JmModuleConfig,
        JmOption,
        JmMagicConstants,
        JmAlbumDetail,
        JmSearchPage,
    )
    HAS_JMCOMIC = True
except ImportError:
    HAS_JMCOMIC = False

def _init_malloc_trim():
    """返回一个把空闲内存归还操作系统的函数；非 Linux/glibc 环境下返回空操作。

    Python 的 gc.collect() 只回收 Python 对象，调用 free() 后 glibc 的 malloc
    通常仍把内存留在进程 arena 里不还给系统，导致 RSS 居高不下。malloc_trim(0)
    会强制收缩 arena 并归还。
    """
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        trim = libc.malloc_trim
        trim.argtypes = [ctypes.c_size_t]
        trim.restype = ctypes.c_int

        def _trim():
            try:
                trim(0)
            except Exception:
                pass

        return _trim
    except Exception:
        return lambda: None


_malloc_trim = _init_malloc_trim()


def _reclaim_memory_sync():
    """Force free Python objects + return free glibc arenas to the OS.

    Call at the end of heavy image/PDF stages. Double-collect helps break
    simple reference cycles before malloc_trim.
    """
    try:
        gc.collect(2)
        gc.collect(2)
    except Exception:
        try:
            gc.collect()
            gc.collect()
        except Exception:
            pass

    # Drop Pillow decoder / open caches if available.
    try:
        from PIL import Image as _PILImage
        if hasattr(_PILImage, "reset_cache"):
            _PILImage.reset_cache()
    except Exception:
        pass
    try:
        from PIL import ImageFile as _PILImageFile
        if hasattr(_PILImageFile, "reset"):
            _PILImageFile.reset()
    except Exception:
        pass

    _malloc_trim()
    # Second trim after a short free-list settle.
    _malloc_trim()


def _file_to_base64_image(filepath: str) -> str:
    """Read image file and return base64 data URL."""
    with open(filepath, "rb") as f:
        raw = f.read()
    data = base64.b64encode(raw).decode("ascii")
    del raw
    ext = os.path.splitext(filepath)[1].lower()
    mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
                ".webp": "image/webp", ".gif": "image/gif", ".bmp": "image/bmp"}
    mime = mime_map.get(ext, "image/jpeg")
    out = f"base64://{data}"
    del data
    return out


def _safe_file_count(path: str) -> int:
    try:
        return sum(len(files) for _, _, files in os.walk(path))
    except Exception:
        return -1


@register(
    "astrbot_plugin_jmcomic",
    "JMComic禁漫搜索",
    "禁漫本子搜索/下载整部漫画ZIP",
    "2.3.0",
)
class JMComicPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self._load_config()
        self._client = None
        self._option = None
        self._cover_cache = OrderedDict()    # album_id → cover_path
        self._cover_cache_limit = 20
        self._bg_tasks: set[asyncio.Task] = set()
        self._cache_cleanup_task: asyncio.Task | None = None
        self._download_semaphore = asyncio.Semaphore(2)
        self._max_download_queue = 4
        self._download_tasks: dict[str, dict] = {}
        self._download_generation: dict[str, int] = {}
        self._executor = ThreadPoolExecutor(max_workers=3)
        self._shutting_down = False

        # 将缓存目录设置在插件目录下，以便持久化保存
        self.plugin_dir = os.path.dirname(os.path.abspath(__file__))
        self.cache_dir = os.path.join(self.plugin_dir, "cache")
        self.temp_dir = os.path.join(self.cache_dir, "temp")
        self.zip_dir = os.path.join(self.cache_dir, "zips")

        os.makedirs(self.temp_dir, exist_ok=True)
        os.makedirs(self.zip_dir, exist_ok=True)

        self._schedule_cache_cleanup()

        if not HAS_JMCOMIC:
            logger.warning("jmcomic 库未安装，插件功能不可用。请执行 pip install jmcomic")
        else:
            self._init_jm_client()

    def _create_bg_task(self, coro, *, name: str) -> asyncio.Task | None:
        if self._shutting_down:
            logger.warning(f"插件正在关闭，拒绝创建后台任务: {name}")
            return None
        task = asyncio.create_task(coro, name=name)
        self._bg_tasks.add(task)
        task.add_done_callback(self._discard_task)
        return task

    def _discard_task(self, task: asyncio.Task):
        self._bg_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"后台任务异常退出 {task.get_name()}: {e}", exc_info=True)

    def _schedule_cache_cleanup(self):
        if self._cache_cleanup_task and not self._cache_cleanup_task.done():
            return self._cache_cleanup_task
        self._cache_cleanup_task = self._create_bg_task(
            self._check_and_clean_cache(),
            name="jmcomic-cache-cleanup",
        )
        return self._cache_cleanup_task

    def _get_next_download_generation(self, album_id: str) -> int:
        generation = self._download_generation.get(album_id, 0) + 1
        self._download_generation[album_id] = generation
        return generation

    def _mark_download_stage(self, token: str, stage: str, **extra):
        state = self._download_tasks.get(token)
        if not state:
            return
        state["stage"] = stage
        state["updated_at"] = time.time()
        state.update(extra)
        logger.info(
            f"[download-state] token={token} album={state.get('album_id')} "
            f"stage={stage} queued={len(self._download_tasks)} extra={extra}"
        )

    def _is_download_stale(self, token: str) -> bool:
        state = self._download_tasks.get(token)
        return not state or state.get("abandoned", False)

    def _abandon_download(self, token: str, reason: str):
        state = self._download_tasks.get(token)
        if not state:
            return
        state["abandoned"] = True
        self._mark_download_stage(token, f"abandoned:{reason}")

    async def _run_blocking_with_timeout(self, func, *args, timeout: float, token: str | None = None, stage: str | None = None):
        loop = asyncio.get_running_loop()
        future = self._executor.submit(func, *args)
        wrapped = asyncio.wrap_future(future, loop=loop)
        try:
            return await asyncio.wait_for(wrapped, timeout=timeout)
        except asyncio.TimeoutError:
            if token and stage:
                self._abandon_download(token, f"{stage}-timeout")
            future.cancel()
            logger.error(f"阻塞任务超时 stage={stage} token={token} timeout={timeout}")
            raise
        except asyncio.CancelledError:
            if token and stage:
                self._abandon_download(token, f"{stage}-cancelled")
            future.cancel()
            raise

    async def _run_blocking(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, func, *args)

    def _log_runtime_state(self, tag: str):
        active_tasks = sum(1 for task in self._bg_tasks if not task.done())
        active_downloads = sum(1 for state in self._download_tasks.values() if not state.get("abandoned", False))
        logger.info(
            f"[{tag}] bg_tasks={active_tasks} active_downloads={active_downloads} "
            f"cover_cache={len(self._cover_cache)} temp_files={_safe_file_count(self.temp_dir)} "
            f"zip_files={_safe_file_count(self.zip_dir)}"
        )

    def _astrobot_data_temp(self):
        candidate = os.path.normpath(os.path.join(self.plugin_dir, "..", "..", "..", "data", "temp"))
        return candidate if os.path.isdir(candidate) else None

    def _reclaim_memory(self, tag: str = ""):
        _reclaim_memory_sync()
        if tag:
            self._log_runtime_state(f"reclaim-{tag}")

    def _schedule_delayed_reclaim(self, tag: str, delays=(5.0, 45.0)):
        """Reclaim again after OneBot/fileseg buffers may have released."""
        if self._shutting_down:
            return
        self._create_bg_task(
            self._delayed_reclaim(tag, delays),
            name=f"jm-reclaim-{tag}",
        )

    async def _delayed_reclaim(self, tag: str, delays=(5.0, 45.0)):
        for delay in delays:
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise
            if self._shutting_down:
                return
            self._reclaim_memory(f"{tag}-t{int(delay)}")

    def _schedule_fileseg_cleanup(self, file_name: str, delay: float = 30.0):
        if self._shutting_down:
            return
        task = self._create_bg_task(
            self._cleanup_fileseg_temp(file_name, delay),
            name=f"jm-fileseg-cleanup-{file_name}",
        )
        if task is None:
            logger.warning(f"cannot start fileseg cleanup for {file_name}")

    async def _cleanup_fileseg_temp(self, file_name: str, delay: float = 30.0):
        await asyncio.sleep(delay)
        stem = os.path.splitext(file_name)[0]
        data_temp = self._astrobot_data_temp()
        if not data_temp:
            return
        pattern = f"fileseg_{stem}_*"
        removed = 0
        for fp in glob.glob(os.path.join(data_temp, pattern)):
            try:
                if os.path.isfile(fp):
                    os.remove(fp)
                    removed += 1
            except Exception:
                pass
        if removed:
            logger.info(f"cleaned {removed} upload chunk(s): {pattern}")
        self._reclaim_memory(f"fileseg-{file_name}")

    def _cleanup_astrobot_fileseg_temp(self, max_age_minutes: int = 30):
        data_temp = self._astrobot_data_temp()
        if not data_temp:
            return
        cutoff = max(1, max_age_minutes) * 60
        now = time.time()
        removed = 0
        for fp in glob.glob(os.path.join(data_temp, "fileseg_JMComic_*")):
            try:
                if now - os.path.getmtime(fp) >= cutoff:
                    os.remove(fp)
                    removed += 1
            except Exception:
                pass
        if removed:
            logger.info(f"periodic sweep removed {removed} JMComic upload chunk(s)")

    async def _shutdown_plugin(self):
        if self._shutting_down:
            return
        self._shutting_down = True

        current_task = asyncio.current_task()
        tasks = [task for task in self._bg_tasks if not task.done() and task is not current_task]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if self._cache_cleanup_task and not self._cache_cleanup_task.done() and self._cache_cleanup_task is not current_task:
            self._cache_cleanup_task.cancel()
            await asyncio.gather(self._cache_cleanup_task, return_exceptions=True)

        self._download_tasks.clear()
        self._cover_cache.clear()
        client = self._client
        self._client = None
        self._option = None
        if client:
            for method_name in ("close", "cleanup"):
                method = getattr(client, method_name, None)
                if callable(method):
                    try:
                        result = method()
                        if inspect.isawaitable(result):
                            await result
                    except Exception as e:
                        logger.warning(f"关闭 JM 客户端失败({method_name}): {e}")
                    break

        self._executor.shutdown(wait=False, cancel_futures=True)
        self._log_runtime_state("shutdown")

    async def terminate(self):
        await self._shutdown_plugin()

    async def cleanup(self):
        await self._shutdown_plugin()

    async def close(self):
        await self._shutdown_plugin()

    async def on_unload(self):
        await self._shutdown_plugin()

    async def _check_and_clean_cache(self):
        """检查缓存目录大小，如果超过 5GB 则清理最旧的文件"""
        try:
            max_size = int(self._cache_max_size_gb * 1024 * 1024 * 1024)

            def get_dir_size(path):
                total = 0
                for dirpath, _, filenames in os.walk(path):
                    for f in filenames:
                        fp = os.path.join(dirpath, f)
                        if not os.path.islink(fp):
                            total += os.path.getsize(fp)
                return total

            current_size = get_dir_size(self.zip_dir)
            if current_size > max_size:
                logger.info(f"缓存目录大小 ({current_size / 1024 / 1024 / 1024:.2f}GB) 超过 {self._cache_max_size_gb}GB，开始清理...")

                files = []
                for f in os.listdir(self.zip_dir):
                    if f.endswith('.zip'):
                        fp = os.path.join(self.zip_dir, f)
                        files.append((fp, os.path.getmtime(fp), os.path.getsize(fp)))

                files.sort(key=lambda x: x[1])

                target_size = int(self._cache_target_size_gb * 1024 * 1024 * 1024)
                for fp, _, size in files:
                    if current_size <= target_size:
                        break
                    try:
                        os.remove(fp)
                        current_size -= size
                        logger.info(f"已清理缓存文件: {fp}")
                    except Exception as e:
                        logger.warning(f"清理缓存文件失败 {fp}: {e}")

            self._cleanup_astrobot_fileseg_temp(max_age_minutes=30)
            self._prune_cover_cache_entries()
            self._cleanup_old_temp_files(max_age_hours=self._temp_file_max_age_hours)
            self._log_runtime_state("cache-cleanup")
        except Exception as e:
            logger.error(f"检查清理缓存异常: {e}")

    def _prune_cover_cache_entries(self):
        stale_album_ids = []
        for album_id, cover_path in self._cover_cache.items():
            if not cover_path or not os.path.exists(cover_path):
                stale_album_ids.append(album_id)
        for album_id in stale_album_ids:
            self._cover_cache.pop(album_id, None)

    def _cleanup_old_temp_files(self, max_age_hours: int = 6):
        cutoff = max(1, max_age_hours) * 3600
        now = time.time()
        for root, _, files in os.walk(self.temp_dir):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                try:
                    if now - os.path.getmtime(file_path) >= cutoff:
                        os.remove(file_path)
                except Exception:
                    pass

    def _remember_cover_path(self, album_id: str, cover_path: str):
        self._cover_cache[album_id] = cover_path
        self._cover_cache.move_to_end(album_id)
        while len(self._cover_cache) > self._cover_cache_limit:
            self._cover_cache.popitem(last=False)

    def _build_send_context(self, event):
        return {
            "bot": self._get_onebot(event),
            "group_id": self._get_group_id(event),
            "user_id": self._get_user_id(event),
            "message_id": getattr(getattr(event, "message_obj", None), "message_id", None),
        }

    async def _send_context_text(self, ctx: dict, text: str):
        bot = ctx.get("bot")
        if not bot:
            logger.error("回复文本失败：未找到 bot 对象")
            return False

        message = []
        message_id = ctx.get("message_id")
        if message_id:
            message.append({"type": "reply", "data": {"id": str(message_id)}})
        message.append({"type": "text", "data": {"text": text}})

        group_id = ctx.get("group_id")
        if group_id:
            await bot.send_group_msg(group_id=int(group_id), message=message)
            return True

        user_id = ctx.get("user_id")
        if user_id:
            await bot.send_private_msg(user_id=int(user_id), message=message)
            return True

        logger.error("回复文本失败：未找到有效的 group_id 或 user_id")
        return False

    async def _upload_onebot_file_by_ctx(self, ctx: dict, file_path: str, file_name: str) -> bool:
        bot = ctx.get("bot")
        if not bot:
            logger.error("OneBot 文件上传失败：未找到 bot 对象")
            return False

        group_id = ctx.get("group_id")
        is_group_message = bool(group_id)
        file_url = f"file:///{os.path.abspath(file_path)}"

        if group_id:
            try:
                await bot.upload_group_file(group_id=int(group_id), file=file_path, name=file_name)
                logger.info(f"OneBot 群文件上传成功 group_id={group_id} file={file_path}")
                return True
            except Exception as e:
                logger.warning(f"OneBot upload_group_file 失败: {e}，尝试使用 send_group_msg 发送文件")
                try:
                    await bot.send_group_msg(
                        group_id=int(group_id),
                        message=[{"type": "file", "data": {"file": file_url, "name": file_name}}]
                    )
                    logger.info(f"OneBot 群文件发送成功 group_id={group_id} file={file_path}")
                    return True
                except Exception as e2:
                    logger.error(f"OneBot 群文件发送失败: {e2}", exc_info=True)
                    return False

        if is_group_message:
            logger.error("OneBot 文件上传失败：群消息未获取到有效群号，不回退私聊")
            return False

        user_id = ctx.get("user_id")
        if user_id:
            try:
                await bot.upload_private_file(user_id=int(user_id), file=file_path, name=file_name)
                logger.info(f"OneBot 私聊文件上传成功 user_id={user_id} file={file_path}")
                return True
            except Exception as e:
                logger.warning(f"OneBot upload_private_file 失败: {e}，尝试使用 send_private_msg 发送文件")
                try:
                    await bot.send_private_msg(
                        user_id=int(user_id),
                        message=[{"type": "file", "data": {"file": file_url, "name": file_name}}]
                    )
                    logger.info(f"OneBot 私聊文件发送成功 user_id={user_id} file={file_path}")
                    return True
                except Exception as e2:
                    logger.error(f"OneBot 私聊文件发送失败: {e2}", exc_info=True)

        return False

    def _load_config(self):
        astr_cfg = getattr(self, "config", {}) or {}
        self._client_impl = astr_cfg.get("client_impl", "api")
        self._proxy = astr_cfg.get("proxy", "")
        self._domains = astr_cfg.get("domains", [])

        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        if os.path.exists(config_path):
            try:
                import yaml
                with open(config_path, "r", encoding="utf-8") as f:
                    file_cfg = yaml.safe_load(f) or {}
                self._client_impl = self._client_impl or file_cfg.get("client_impl", "api")
                self._proxy = self._proxy or file_cfg.get("proxy", "")
                self._domains = self._domains or file_cfg.get("domains", [])
            except ImportError:
                pass

        def _cfg_int(key, default):
            try:
                return int(astr_cfg.get(key, default))
            except (TypeError, ValueError):
                return default

        def _cfg_float(key, default):
            try:
                return float(astr_cfg.get(key, default))
            except (TypeError, ValueError):
                return default

        # 网络/超时（秒）
        self._retry_times = _cfg_int("retry_times", 3)
        self._timeout_request = _cfg_int("timeout_request", 30)
        self._timeout_download_photo = _cfg_int("timeout_download_photo", 300)
        self._timeout_compress = _cfg_int("timeout_compress", 300)
        self._timeout_pdf = _cfg_int("timeout_pdf", 300)
        self._timeout_zip = _cfg_int("timeout_zip", 180)

        # 下载限制
        self._max_images = _cfg_int("max_images", 400)
        self._search_result_limit = _cfg_int("search_result_limit", 20)

        # 缓存清理
        self._cache_max_size_gb = _cfg_float("cache_max_size_gb", 5.0)
        self._cache_target_size_gb = _cfg_float("cache_target_size_gb", 4.0)
        self._temp_file_max_age_hours = _cfg_int("temp_file_max_age_hours", 6)

    def _init_jm_client(self):
        try:
            JmModuleConfig.FLAG_ENABLE_JM_LOG = False
            option_dict = {
                "log": False,
                "dir_rule": {"rule": "Bd_Aid", "base_dir": self.temp_dir},
                "download": {
                    "cache": False,
                    "image": {"decode": True, "suffix": None},
                    "threading": {"image": 10, "photo": 1},
                },
                "client": {
                    "cache": None,
                    "domain": self._domains if self._domains else [],
                    "postman": {
                        "type": "curl_cffi",
                        "meta_data": {
                            "impersonate": "chrome",
                            "headers": None,
                            "proxies": self._proxy if self._proxy else None,
                        },
                    },
                    "impl": self._client_impl,
                    "retry_times": self._retry_times,
                },
            }
            self._option = JmOption.construct(option_dict)
            self._client = self._option.build_jm_client()
            logger.info(f"JMComic 客户端初始化成功 (impl={self._client_impl})")
        except Exception as e:
            logger.error(f"JMComic 客户端初始化失败: {e}")

    def _parse_kw(self, msg: str, prefix: str) -> str:
        clean = msg.strip()
        if clean.startswith("/"):
            clean = clean[1:]
        if clean.startswith(prefix + " "):
            return clean[len(prefix) + 1:].strip()
        if clean == prefix:
            return ""
        if clean.startswith(prefix):
            return clean[len(prefix):].strip()
        return clean

    async def _download_cover(self, album_id: str, save_path: str) -> bool:
        if not self._client:
            return False
        try:
            def do_download():
                try:
                    self._client.download_album_cover(album_id, save_path)
                    return True
                except Exception as e:
                    logger.warning(f"封面下载失败: {e}")
                    return False

            return await self._run_blocking_with_timeout(
                do_download,
                timeout=self._timeout_request,
                stage="cover",
            )
        except Exception as e:
            logger.error(f"封面下载异常: {e}")
            return False

    async def _get_cover_path(self, album_id: str) -> str | None:
        """Return local cover file path (preferred for forwarding; avoids huge base64)."""
        cover_path = self._cover_cache.get(album_id)
        if cover_path and os.path.exists(cover_path) and os.path.getsize(cover_path) > 0:
            self._cover_cache.move_to_end(album_id)
            return cover_path

        cover_path = os.path.join(self.temp_dir, f"cover_{album_id}.jpg")
        if os.path.exists(cover_path) and os.path.getsize(cover_path) > 0:
            self._remember_cover_path(album_id, cover_path)
            return cover_path

        has_cover = await self._download_cover(album_id, cover_path)
        if has_cover and os.path.exists(cover_path) and os.path.getsize(cover_path) > 0:
            self._remember_cover_path(album_id, cover_path)
            return cover_path
        return None

    async def _get_cover_base64(self, album_id: str) -> str | None:
        cover_path = await self._get_cover_path(album_id)
        if not cover_path:
            return None
        return _file_to_base64_image(cover_path)

    def _search_page_sync(self, page_num: int, keyword: str) -> list:
        """同步搜索单页（在线程池中运行）"""
        try:
            page: JmSearchPage = self._client.search(
                search_query=keyword,
                page=page_num,
                main_tag=0,
                order_by=JmMagicConstants.ORDER_BY_LATEST,
                time=JmMagicConstants.TIME_ALL,
                category=JmMagicConstants.CATEGORY_ALL,
                sub_category=None,
            )
            return [{"album_id": aid, "name": name} for aid, name in page.iter_id_title()]
        except Exception as e:
            logger.error(f"第{page_num}页搜索失败: {e}")
            if page_num == 1:
                raise
            return []

    async def _jm_search(self, keyword: str) -> list:
        """搜索"""
        if not self._client:
            return []
        try:
            return await self._run_blocking_with_timeout(
                self._search_page_sync,
                1, keyword,
                timeout=self._timeout_request,
                stage="search",
            )
        except asyncio.TimeoutError:
            logger.error(f"搜索超时: {keyword}")
            return []
        except Exception as e:
            logger.error(f"搜索异常: {e}")
            return []

    async def _jm_get_detail(self, album_id: str, token: str | None = None) -> JmAlbumDetail | None:
        if not self._client:
            return None
        try:
            if token:
                self._mark_download_stage(token, "detail")

            def do_get():
                try:
                    logger.info(f"正在获取本子详情 JM{album_id}")
                    return self._client.get_album_detail(album_id)
                except Exception as e:
                    logger.error(f"获取详情失败: {e}")
                    return None

            result = await self._run_blocking_with_timeout(
                do_get,
                timeout=self._timeout_request,
                token=token,
                stage="detail",
            )
            logger.info(f"获取本子详情成功 JM{album_id}")
            return result
        except asyncio.TimeoutError:
            logger.error(f"获取本子详情超时 JM{album_id}")
            return None
        except Exception as e:
            logger.error(f"获取详情异常: {e}")
            return None

    # ============ /jm ============
    @filter.command("jm")
    async def jm_main(self, event):
        keyword = self._parse_kw(event.message_str, "jm")
        async for result in self._handle_jm(event, keyword):
            yield result

    @filter.regex(r"^.*jm\s*\S+.*$")
    async def jm_plain(self, event):
        keyword = self._parse_jm_message(event)
        if keyword is None:
            return
        async for result in self._handle_jm(event, keyword):
            yield result

    def _parse_jm_message(self, event) -> str | None:
        parts = []
        try:
            for seg in event.get_messages():
                if isinstance(seg, Plain):
                    parts.append(seg.text)
        except Exception:
            parts.append(event.message_str)
        clean = " ".join(parts).strip()
        if not clean:
            clean = event.message_str.strip()
        clean = clean.replace("/", " ").strip()
        tokens = clean.split()
        for index, token in enumerate(tokens):
            if token == "jm":
                return " ".join(tokens[index + 1:]).strip()
            if token.startswith("jm") and len(token) > 2:
                return token[2:].strip()
        return None

    async def _handle_jm(self, event, keyword: str):
        if not HAS_JMCOMIC:
            yield event.plain_result("JMComic 库未安装，请联系管理员安装 jmcomic。")
            return
        if not self._client:
            yield event.plain_result("JMComic 客户端未初始化，请检查配置。")
            return

        if not keyword:
            yield event.plain_result("请输入搜索关键词或本子ID，例如：/jm 全彩 或 /jm350234")
            return

        event.stop_event()

        if keyword.isdigit():
            active_downloads = sum(1 for state in self._download_tasks.values() if not state.get("abandoned", False))
            if active_downloads >= self._max_download_queue:
                yield event.plain_result(f"当前下载任务较多（{active_downloads} 个），请稍后再试。")
                return

            send_ctx = self._build_send_context(event)
            generation = self._get_next_download_generation(keyword)
            token = f"{keyword}:{generation}:{uuid.uuid4().hex[:8]}"
            self._download_tasks[token] = {
                "album_id": keyword,
                "generation": generation,
                "stage": "queued",
                "created_at": time.time(),
                "updated_at": time.time(),
                "abandoned": False,
            }
            task = self._create_bg_task(
                self._background_download_and_send(send_ctx, keyword, token),
                name=f"jm-download-{keyword}-{generation}",
            )
            if task is None:
                self._download_tasks.pop(token, None)
                yield event.plain_result("插件正在关闭，暂时无法创建下载任务。")
                return
            self._mark_download_stage(token, "queued")
            yield event.plain_result(f"开始下载 JM{keyword}，请稍等...")
            return

        # IMPORTANT: do NOT yield intermediate messages then continue this generator.
        # astrbot_plugin_recall_cancel terminates event propagation after the first
        # message is sent (after_message_sent), which would abort the rest of search.
        # Mirror the download path: ack once, then finish work in a background task.
        send_ctx = self._build_send_context(event)
        task = self._create_bg_task(
            self._background_search_and_send(send_ctx, keyword),
            name=f"jm-search-{keyword[:32]}",
        )
        if task is None:
            yield event.plain_result("插件正在关闭，暂时无法搜索。")
            return
        yield event.plain_result(f"正在搜索「{keyword}」...")
        return

    async def _background_search_and_send(self, send_ctx: dict, keyword: str):
        """Run search off the event pipeline so recall_cancel cannot abort it.

        Text-only results: no cover download / no merge-forward.
        """
        try:
            results = await self._jm_search(keyword)
            if not results:
                await self._send_context_text(
                    send_ctx,
                    f"未找到与「{keyword}」相关的本子。",
                )
                return

            show = results[:self._search_result_limit]
            lines_text = [
                f"搜索「{keyword}」共 {len(results)} 条，展示前 {len(show)} 条："
            ]
            for i, r in enumerate(show):
                album_id = r["album_id"]
                name = r.get("name", "未知")
                lines_text.append(f"{i + 1}. {name}\n/jm{album_id}")
            await self._send_context_text(send_ctx, "\n".join(lines_text))
            self._log_runtime_state("search-finished")
            self._schedule_delayed_reclaim("search-finished", delays=(5.0,))
        except Exception as e:
            logger.error(f"background search failed: {e}", exc_info=True)
            try:
                await self._send_context_text(send_ctx, f"搜索失败: {e}")
            except Exception:
                pass

    async def _background_download_and_send(self, send_ctx: dict, album_id: str, token: str):
        """后台执行下载并发送结果，避免被其他插件拦截导致中断"""
        uploaded_file_name = None
        try:
            async with self._download_semaphore:
                if self._is_download_stale(token):
                    return
                self._mark_download_stage(token, "started")
                self._log_runtime_state(f"download-start-{album_id}")
                album = await self._jm_get_detail(album_id, token=token)
                if self._is_download_stale(token):
                    await self._send_context_text(send_ctx, f"JM{album_id} 下载超时或已取消，请稍后重试。")
                    return
                if not album:
                    await self._send_context_text(send_ctx, f"未找到本子 JM{album_id}")
                    return

                max_images = self._max_images
                self._mark_download_stage(token, "estimating")
                estimated_images = await self._estimate_album_images(album, token=token)
                if self._is_download_stale(token):
                    await self._send_context_text(send_ctx, f"JM{album_id} 下载超时或已取消，请稍后重试。")
                    return
                if estimated_images is not None and estimated_images > max_images:
                    await self._send_context_text(
                        send_ctx,
                        f"JM{album_id}「{album.name}」预计约 {estimated_images} 张图，超过当前上限 {max_images} 张。\n为防止内存溢出和 Bot 卡死，已拒绝下载。",
                    )
                    return

                self._mark_download_stage(token, "checking-cache")
                cached_zip_path = None
                for f in os.listdir(self.zip_dir):
                    if f.startswith(f"{album_id}_") and f.endswith(".zip"):
                        cached_zip_path = os.path.join(self.zip_dir, f)
                        break

                if cached_zip_path and os.path.exists(cached_zip_path):
                    logger.info(f"找到本地缓存的 ZIP 文件: {cached_zip_path}")
                    album_name = album.name
                    file_name = f"JMComic_{album_id}.zip"

                    self._mark_download_stage(token, "uploading-cached")
                    sent = await self._upload_onebot_file_by_ctx(send_ctx, cached_zip_path, file_name)
                    if sent:
                        uploaded_file_name = file_name
                        await self._send_context_text(send_ctx, f"「{album_name}」发送完成 (来自缓存)\n解压密码：{album_id}")
                    else:
                        await self._send_context_text(send_ctx, f"文件发送失败：{cached_zip_path}")
                    return

                result = await self._download_album(album_id, album, token=token)
                if self._is_download_stale(token):
                    await self._send_context_text(send_ctx, f"JM{album_id} 下载超时或已取消，请稍后重试。")
                    return
                if result["ok"]:
                    self._mark_download_stage(token, "uploading")
                    sent = await self._upload_onebot_file_by_ctx(send_ctx, result["file_path"], result["file_name"])
                    if sent:
                        uploaded_file_name = result["file_name"]
                        await self._send_context_text(send_ctx, result["message"])
                    else:
                        await self._send_context_text(send_ctx, f"{result['message']}\n文件已生成，但发送失败：{result['file_path']}")
                else:
                    await self._send_context_text(send_ctx, result["message"])
        except Exception as e:
            logger.error(f"后台下载任务异常: {e}", exc_info=True)
            try:
                await self._send_context_text(send_ctx, f"下载任务发生异常: {e}")
            except Exception:
                pass
        finally:
            if uploaded_file_name:
                self._schedule_fileseg_cleanup(uploaded_file_name, delay=30.0)
            self._reclaim_memory(f"download-finished-{album_id}")
            self._schedule_delayed_reclaim(f"download-finished-{album_id}", delays=(5.0, 45.0))
            self._mark_download_stage(token, "finished")
            self._download_tasks.pop(token, None)
            # 该 album 已无进行中的任务时，清理 generation 记录，避免长期运行无限增长
            if not any(s.get("album_id") == album_id for s in self._download_tasks.values()):
                self._download_generation.pop(album_id, None)
            self._log_runtime_state(f"download-finished-{album_id}")

    async def _estimate_album_images(self, album: JmAlbumDetail, token: str | None = None) -> int | None:
        total = 0
        for ep in album.episode_list:
            if token and self._is_download_stale(token):
                return None
            photo_id = ep[0]
            try:
                photo = await self._jm_get_photo_detail(photo_id, token=token)
            except Exception:
                return None
            if not photo:
                return None
            image_count = self._extract_photo_image_count(photo)
            if image_count is None:
                return None
            total += image_count
            if token:
                self._mark_download_stage(token, "estimating", estimated_images=total)
            if total > self._max_images:
                return total
        return total

    async def _jm_get_photo_detail(self, photo_id: str, token: str | None = None):
        if not self._client:
            return None
        if token and self._is_download_stale(token):
            return None

        def do_get():
            return self._client.get_photo_detail(photo_id)

        return await self._run_blocking_with_timeout(
            do_get,
            timeout=self._timeout_request,
            token=token,
            stage="photo-detail",
        )

    @staticmethod
    def _extract_photo_image_count(photo) -> int | None:
        for attr_name in ("page_arr", "img_url_list", "image_url_list", "img_urls", "images"):
            value = getattr(photo, attr_name, None)
            if value is not None:
                try:
                    return len(value)
                except Exception:
                    pass
        for attr_name in ("page_count", "img_count", "image_count"):
            value = getattr(photo, attr_name, None)
            if value is not None:
                try:
                    return int(value)
                except Exception:
                    pass
        return None

    async def _download_album(self, album_id: str, album: JmAlbumDetail, token: str | None = None) -> dict:
        try:
            album_name = album.name
            episode_count = len(album.episode_list)
            if episode_count == 0:
                return {"ok": False, "message": f"JM{album_id} 没有可下载章节"}

            dl_dir = os.path.join(self.temp_dir, f"album_{album_id}_{uuid.uuid4().hex[:8]}")
            os.makedirs(dl_dir, exist_ok=True)
            pdf_path = None

            chapter_files = []

            for idx, ep in enumerate(album.episode_list):
                if token and self._is_download_stale(token):
                    return {"ok": False, "message": "下载任务已超时或取消。"}
                photo_id = ep[0]
                ep_index = ep[1]
                ep_name = ep[2]
                chapter_name = f"第{ep_index}话 {ep_name}"
                logger.info(f"正在下载 JM{album_id} {idx + 1}/{episode_count} {chapter_name} photo_id={photo_id}")
                if token:
                    self._mark_download_stage(token, "downloading", chapter=f"{idx + 1}/{episode_count}", photo_id=photo_id)

                ep_dir = os.path.join(dl_dir, f"ep_{ep_index}")
                os.makedirs(ep_dir, exist_ok=True)

                def do_download():
                    try:
                        temp_opt = self._option.copy_option()
                        temp_opt.dir_rule.base_dir = ep_dir
                        temp_opt.dir_rule.rule_dsl = "Bd"
                        temp_opt.download.threading.image = 10
                        jmcomic.download_photo(photo_id, temp_opt, check_exception=False)
                        return True
                    except Exception as e:
                        logger.error(f"下载章节 {photo_id} 失败: {e}")
                        return False

                success = await self._run_blocking_with_timeout(
                    do_download,
                    timeout=self._timeout_download_photo,
                    token=token,
                    stage="download-photo",
                )
                if token and self._is_download_stale(token):
                    return {"ok": False, "message": "下载任务已超时或取消。"}
                if not success:
                    logger.warning(f"章节 {chapter_name} 下载失败，跳过")
                    continue

                exts = ("*.jpg", "*.jpeg", "*.png", "*.webp", "*.gif", "*.bmp")
                current_files = []
                for ext in exts:
                    current_files.extend(glob.glob(os.path.join(ep_dir, "**", ext), recursive=True))
                chapter_files.extend(sorted(current_files))

            if not chapter_files:
                return {"ok": False, "message": "下载完成但未找到图片文件。"}

            total_images = len(chapter_files)
            MAX_IMAGES = self._max_images
            if total_images > MAX_IMAGES:
                logger.warning(f"JM{album_id} 图片数量过多({total_images})，超过上限 {MAX_IMAGES}，停止生成文件")
                return {"ok": False, "message": f"JM{album_id}「{album_name}」共 {total_images} 张图，超过当前上限 {MAX_IMAGES} 张。\n为防止内存溢出和 Bot 卡死，已拒绝下载。"}

            logger.info(f"JM{album_id} 下载完成，共 {total_images} 张图片，开始生成PDF")

            compressed_dir = os.path.join(dl_dir, "compressed")

            if token:
                self._mark_download_stage(token, "compressing", images=total_images)
            image_files = await self._run_blocking_with_timeout(
                self._compress_images,
                chapter_files, compressed_dir, 75, 1600,
                timeout=self._timeout_compress,
                token=token,
                stage="compress",
            )
            del chapter_files
            self._reclaim_memory(f"compress-done-{album_id}")
            if token and self._is_download_stale(token):
                return {"ok": False, "message": "下载任务已超时或取消。"}

            pdf_name = f"{uuid.uuid4().hex[:8]}.pdf"
            pdf_path = os.path.join(self.temp_dir, pdf_name)

            if token:
                self._mark_download_stage(token, "pdf")
            pdf_success = await self._run_blocking_with_timeout(
                self._images_to_pdf,
                image_files, pdf_path,
                timeout=self._timeout_pdf,
                token=token,
                stage="pdf",
            )
            del image_files
            self._reclaim_memory(f"pdf-done-{album_id}")
            if not pdf_success:
                return {"ok": False, "message": "PDF 生成失败。"}
            if token and self._is_download_stale(token):
                return {"ok": False, "message": "下载任务已超时或取消。"}

            zip_name = f"JMComic_{album_id}.zip"
            zip_path = os.path.join(self.zip_dir, f"{album_id}_{zip_name}")

            if token:
                self._mark_download_stage(token, "zip")
            zip_success = await self._run_blocking_with_timeout(
                self._pdf_to_zip,
                pdf_path, zip_path, album_id,
                timeout=self._timeout_zip,
                token=token,
                stage="zip",
            )
            if not zip_success:
                return {"ok": False, "message": "ZIP 打包失败，请检查是否安装了 pyzipper。"}

            try:
                os.remove(pdf_path)
            except Exception:
                pass
            pdf_path = None
            self._reclaim_memory(f"zip-done-{album_id}")

            self._schedule_cache_cleanup()
            self._log_runtime_state(f"album-downloaded-{album_id}")

            return {
                "ok": True,
                "message": f"「{album_name}」下载完成：{episode_count}话 {total_images}图\n解压密码：{album_id}",
                "file_path": zip_path,
                "file_name": zip_name,
            }

        except asyncio.TimeoutError:
            logger.error(f"下载本子超时 JM{album_id}")
            return {"ok": False, "message": "下载超时，请稍后再试"}
        except Exception as e:
            logger.error(f"下载本子异常: {e}", exc_info=True)
            return {"ok": False, "message": f"下载失败: {e}"}
        finally:
            try:
                if 'dl_dir' in locals() and os.path.exists(dl_dir):
                    shutil.rmtree(dl_dir, ignore_errors=True)
            except Exception:
                pass
            # Drop local refs that may still hold large path lists / options.
            try:
                chapter_files = None  # noqa: F841
                image_files = None  # noqa: F841
            except Exception:
                pass
            _reclaim_memory_sync()

    async def _upload_onebot_file(self, event, file_path: str, file_name: str) -> bool:
        return await self._upload_onebot_file_by_ctx(self._build_send_context(event), file_path, file_name)

    def _get_onebot(self, event):
        bot = self._get_event_attr(event, ("bot", "_bot"))
        message_obj = getattr(event, "message_obj", None)
        if not bot and message_obj:
            bot = self._get_event_attr(message_obj, ("bot", "_bot"))
        return bot

    def _get_group_id(self, event):
        message_obj = getattr(event, "message_obj", None)
        group_id = self._get_event_value(event, ("get_group_id",), ("group_id", "group"))
        if not group_id and message_obj:
            group_id = self._get_event_value(message_obj, (), ("group_id", "group"))
            if not group_id:
                session_id = self._get_event_value(message_obj, (), ("session_id",))
                if session_id and "_" in str(session_id):
                    group_id = str(session_id).split("_")[-1]
        return group_id

    def _get_user_id(self, event):
        message_obj = getattr(event, "message_obj", None)
        user_id = self._get_event_value(event, ("get_sender_id",), ("user_id", "sender_id"))
        if not user_id and message_obj:
            user_id = self._get_event_value(message_obj, (), ("user_id", "sender_id"))
        return user_id

    @staticmethod
    def _get_event_attr(obj, attr_names: tuple):
        for attr_name in attr_names:
            value = getattr(obj, attr_name, None)
            if value:
                return value
        return None

    @staticmethod
    def _get_event_value(obj, method_names: tuple, attr_names: tuple):
        for method_name in method_names:
            method = getattr(obj, method_name, None)
            if callable(method):
                try:
                    value = method()
                    if value:
                        return value
                except Exception:
                    pass
        for attr_name in attr_names:
            value = getattr(obj, attr_name, None)
            if value:
                return value
        return None

    @staticmethod
    async def _delayed_cleanup(filepath: str, delay: float = 5.0):
        await asyncio.sleep(delay)
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except Exception:
            pass

    @staticmethod
    def _compress_images(image_paths: list, output_dir: str, quality: int = 75, max_edge: int = 1600) -> list:
        try:
            from PIL import Image as PILImage
        except ImportError:
            return image_paths

        os.makedirs(output_dir, exist_ok=True)
        compressed = []
        for i, path in enumerate(image_paths, 1):
            converted = None
            resized = None
            output = None
            try:
                with PILImage.open(path) as img:
                    source = img
                    if source.mode not in ("RGB", "RGBA"):
                        converted = source.convert("RGB")
                        source = converted
                    w, h = source.size
                    if max(w, h) > max_edge:
                        ratio = max_edge / max(w, h)
                        resized = source.resize((int(w * ratio), int(h * ratio)), PILImage.LANCZOS)
                        source = resized
                    basename = os.path.splitext(os.path.basename(path))[0] + ".jpg"
                    out_path = os.path.join(output_dir, basename)
                    output = source.convert("RGB")
                    output.save(out_path, "JPEG", quality=quality, optimize=True)
                    compressed.append(out_path)
            except Exception as e:
                logger.warning(f"compress image failed {path}: {e}, use original")
                compressed.append(path)
            finally:
                if output is not None:
                    try:
                        output.close()
                    except Exception:
                        pass
                if converted is not None:
                    try:
                        converted.close()
                    except Exception:
                        pass
                if resized is not None:
                    try:
                        resized.close()
                    except Exception:
                        pass
                del converted, resized, output
                # Periodic reclaim to prevent arena growth across hundreds of images.
                if i % 20 == 0:
                    _reclaim_memory_sync()

        _reclaim_memory_sync()
        return compressed

    @staticmethod
    def _images_to_pdf(image_paths: list, output_path: str) -> bool:
        if not image_paths:
            return False
        try:
            import img2pdf
            with open(output_path, "wb") as f:
                img2pdf.convert(image_paths, outputstream=f)
            _reclaim_memory_sync()
            return True
        except ImportError:
            pass
        except Exception as e:
            logger.warning(f"img2pdf convert failed, fallback to Pillow: {e}")

        try:
            from PIL import Image as PILImage
            chunk_size = 10
            chunks = [image_paths[i:i + chunk_size] for i in range(0, len(image_paths), chunk_size)]
            temp_pdfs = []
            for ci, chunk in enumerate(chunks):
                images = []
                try:
                    for path in chunk:
                        with PILImage.open(path) as img:
                            if img.mode == "RGB":
                                images.append(img.copy())
                            else:
                                images.append(img.convert("RGB"))
                    chunk_pdf = output_path + f".chunk{ci}.pdf"
                    if len(images) == 1:
                        images[0].save(chunk_pdf, "PDF")
                    else:
                        images[0].save(chunk_pdf, "PDF", save_all=True, append_images=images[1:])
                    temp_pdfs.append(chunk_pdf)
                finally:
                    for img in images:
                        try:
                            img.close()
                        except Exception:
                            pass
                    images.clear()
                    del images
                    # Reclaim after every chunk, not only at the end.
                    _reclaim_memory_sync()
            if len(temp_pdfs) == 1:
                shutil.move(temp_pdfs[0], output_path)
            else:
                if not JMComicPlugin._merge_pdfs(temp_pdfs, output_path):
                    return False
                for tp in temp_pdfs:
                    try:
                        os.remove(tp)
                    except Exception:
                        pass
            del temp_pdfs, chunks
            _reclaim_memory_sync()
            return True
        except ImportError:
            logger.error("Pillow is not installed, cannot generate PDF.")
            return False
        except Exception as e:
            logger.error(f"PDF generation failed: {e}")
            return False

    @staticmethod
    def _merge_pdfs(input_paths: list, output_path: str) -> bool:
        merger = None
        try:
            from PyPDF2 import PdfMerger
            merger = PdfMerger()
            for path in input_paths:
                merger.append(path)
            merger.write(output_path)
            return True
        except ImportError:
            pass
        except Exception as e:
            logger.warning(f"PyPDF2 合并失败: {e}")
        finally:
            if merger:
                try:
                    merger.close()
                except Exception:
                    pass
            merger = None
            _reclaim_memory_sync()
        logger.error("无法合并 PDF 分块，请安装 PyPDF2。")
        return False

    @staticmethod
    def _pdf_to_zip(pdf_path: str, zip_path: str, password: str) -> bool:
        try:
            import pyzipper
            pdf_name = os.path.basename(pdf_path)
            with pyzipper.AESZipFile(
                zip_path, 'w', compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES
            ) as zf:
                zf.setpassword(password.encode('utf-8'))
                zf.write(pdf_path, pdf_name)
            _reclaim_memory_sync()
            return True
        except ImportError:
            logger.error("pyzipper 库未安装，无法生成加密 ZIP。请执行 pip install pyzipper")
            return False
        except Exception as e:
            logger.error(f"ZIP 打包失败: {e}")
            return False
