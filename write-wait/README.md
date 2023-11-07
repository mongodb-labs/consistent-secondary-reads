To run a series of experiments:

```
dvc exp run --queue -S 'write_wait.write_wait=range(0, 20, 1)'
dvc exp run --run-all
```

