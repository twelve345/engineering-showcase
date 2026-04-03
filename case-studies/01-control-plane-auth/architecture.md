# Architecture

- Separate the control plane from tenant-facing routes.
- Use a dedicated architect auth contract instead of role-switching inside the tenant app.
- Hand off into tenant surfaces explicitly rather than relying on shared browser state.
- Treat shadow access as privileged behavior with focused misuse-oriented tests.
