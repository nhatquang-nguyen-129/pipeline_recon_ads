## Dependency Conflict Management

- Edit `base.in` to change dependencies

- Install pip-tools
```bash
pip install pip-tools
```

- Run `pip-compile` update lock file
```bash
pip-compile requirements/base.in -o requirements/base.txt
```

- Install requirements
```bash
pip install -r requirements/base.txt
```

- Do NOT edit `.txt` files manually to avoid conflict