# MIT6824

## vscode go 环境配置

[https://cloud.tencent.com/developer/article/1590274](https://cloud.tencent.com/developer/article/1590274)

[https://l2m2.top/2020/05/26/2020-05-26-fix-golang-tools-failed-on-vscode/](https://l2m2.top/2020/05/26/2020-05-26-fix-golang-tools-failed-on-vscode/)

默认会自动format import

修改vscode配置可以关闭这个feature

```bash
"editor.codeActionsOnSave": {
	"source.organizeImports": false,
},
```