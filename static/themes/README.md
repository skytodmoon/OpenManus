# OpenManus 主题系统

本文件夹包含OpenManus的不同用户界面主题。

## 主题结构

每个主题必须遵循以下文件结构：

```
主题名称/
  ├── static/
  │   ├── style.css  (必需)
  │   ├── main.js    (必需)
  │   └── ...        (其他静态资源)
  ├── templates/
  │   └── chat.html  (必需)
  └── theme.json     (必需)
```

## 如何添加新主题

1. 在`themes`文件夹中创建一个新文件夹，使用你的主题名称
2. 复制`static`和`templates`文件夹结构
3. 创建`theme.json`文件，包含以下内容：

```json
{
    "name": "主题的中文名称",
    "description": "主题的简短描述",
    "author": "作者名",
    "version": "1.0.0"
}
```

4. 修改CSS和HTML文件以实现你想要的外观

## 路径引用

在`chat.html`中，确保使用以下路径格式引用资源：

```html
<link rel="stylesheet" href="/static/themes/你的主题名称/static/style.css">
<script src="/static/themes/你的主题名称/static/main.js"></script>
```

## 示例主题

- `Normal`: 默认主题
- `cyberpunk`: 赛博朋克主题

当你添加新主题后，系统会自动在首页显示它作为可选项。
