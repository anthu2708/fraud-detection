# ArgoCD Connect

Connect to the local ArgoCD instance running in the k3d fraud-detection cluster.

## Steps

1. Set KUBECONFIG:
```powershell
$env:KUBECONFIG = "C:\Users\phunghuyhau\.config\k3d\kubeconfig-fraud-detection.yaml"
```

2. Check ArgoCD pods are running:
```powershell
kubectl get pods -n argocd
```

3. Get current app sync status:
```powershell
kubectl get applications -n argocd
```

4. Get admin password:
```powershell
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | ForEach-Object { [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($_)) }
```

5. Start port-forward in background (tell user to run this in a separate terminal to keep it alive):
```
kubectl port-forward svc/argocd-server -n argocd 8080:443
```

6. Print a summary to the user:
- UI URL: https://localhost:8080
- Username: admin
- Password: (from step 4)
- App statuses from step 3
- Remind user: open separate terminal for port-forward, accept SSL warning in browser
