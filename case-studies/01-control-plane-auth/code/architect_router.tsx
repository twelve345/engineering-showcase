import { createBrowserRouter, Navigate } from "react-router-dom";

export const architectRouter = createBrowserRouter([
  {
    path: "/login",
    lazy: () =>
      import("@/features/architect/pages/ArchitectLoginPage").then((m) => ({
        Component: m.ArchitectLoginPage,
      })),
  },
  {
    path: "/",
    lazy: () =>
      import("@/features/architect/components/ArchitectLayout").then((m) => ({
        Component: m.ArchitectLayout,
      })),
    children: [
      {
        index: true,
        element: <Navigate to="/dashboard" replace />,
      },
      {
        path: "dashboard",
        lazy: () =>
          import("@/features/architect/pages/DashboardPage").then((m) => ({
            Component: m.DashboardPage,
          })),
      },
      {
        path: "tenants",
        lazy: () =>
          import("@/features/architect/pages/TenantsPage").then((m) => ({
            Component: m.TenantsPage,
          })),
      },
      {
        path: "tenants/:tenantId",
        lazy: () =>
          import("@/features/architect/pages/TenantDetailPage").then((m) => ({
            Component: m.TenantDetailPage,
          })),
      },
      {
        path: "users",
        lazy: () =>
          import("@/features/architect/pages/UsersPage").then((m) => ({
            Component: m.UsersPage,
          })),
      },
      {
        path: "users/:userId",
        lazy: () =>
          import("@/features/architect/pages/UserDetailPage").then((m) => ({
            Component: m.UserDetailPage,
          })),
      },
      {
        path: "audit",
        lazy: () =>
          import("@/features/architect/pages/AuditPage").then((m) => ({
            Component: m.AuditPage,
          })),
      },
      {
        path: "ops/providers",
        lazy: () =>
          import("@/features/architect/pages/OpsProvidersPage").then((m) => ({
            Component: m.OpsProvidersPage,
          })),
      },
      {
        path: "ops/jobs",
        lazy: () =>
          import("@/features/architect/pages/OpsJobsPage").then((m) => ({
            Component: m.OpsJobsPage,
          })),
      },
      {
        path: "ops/jobs/:jobId",
        lazy: () =>
          import("@/features/architect/pages/OpsJobDetailPage").then((m) => ({
            Component: m.OpsJobDetailPage,
          })),
      },
      {
        path: "ops/emails",
        lazy: () =>
          import("@/features/architect/pages/OpsEmailsPage").then((m) => ({
            Component: m.OpsEmailsPage,
          })),
      },
      {
        path: "ops/emails/:emailId",
        lazy: () =>
          import("@/features/architect/pages/OpsEmailDetailPage").then((m) => ({
            Component: m.OpsEmailDetailPage,
          })),
      },
      {
        path: "security",
        lazy: () =>
          import("@/features/architect/pages/SecurityPage").then((m) => ({
            Component: m.SecurityPage,
          })),
      },
      {
        path: "*",
        element: <Navigate to="/dashboard" replace />,
      },
    ],
  },
]);
