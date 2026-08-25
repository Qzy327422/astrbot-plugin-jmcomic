# AstrBot JMComic 禁漫搜索插件

从禁漫天堂搜索本子，支持分页结果（合并转发封面+名字）、查看详情、下载整部漫画为加密 ZIP。

基于 [JMComic-Crawler-Python](https://github.com/hect0x7/JMComic-Crawler-Python) 库实现。

## 安装

1. 将本插件文件夹放入 AstrBot 的 `data/plugins/` 目录
2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```
3. 重启 AstrBot

## 配置

按需修改：

```yaml
client_impl: "api"   # "api"(推荐,不限IP) 或 "html"(需特定地区IP)
proxy: ""            # 代理，如 http://127.0.0.1:7890
domains: []          # 自定义域名，留空使用默认
```

## 命令速查

| 命令 | 说明 |
|------|------|
| `/jm <关键词>` | 搜索本子，合并转发封面+名字 |
| `/jm <本子ID>` | 直接按禁漫ID下载整部漫画为加密 ZIP（解压密码为本子ID） |

## 完整使用演示

### 1. 搜索本子

```
/jm 少女
```

Bot 返回合并转发消息，包含搜索结果（封面图 + 名字 + 本子ID）。

### 2. 下载本子图片 — 加密 ZIP

```
/jm 123456
```

Bot 会下载该本子的所有章节，生成 PDF 后打包为加密 ZIP 发送。解压密码即为本子 ID（如 `123456`）。

## 依赖

- jmcomic >= 2.6.0
- img2pdf >= 0.4.4（流式图片转PDF）
- Pillow >= 9.0.0（img2pdf 不可用时备选）
- PyPDF2 >= 3.0.0（Pillow 模式下合并PDF分块）
- pyzipper >= 0.3.6（将PDF打包为加密ZIP）
