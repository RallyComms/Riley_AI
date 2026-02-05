import type { ReactNode } from "react";
import "@app/styles/globals.css";

export const metadata = {
  title: "Project RILEY",
  description: "Collaborative Intelligence Platform for campaign teams.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className="h-full">
      <body className="min-h-screen bg-zinc-950 text-zinc-100 antialiased">
        <div className="relative flex min-h-screen flex-col bg-gradient-to-b from-zinc-950 via-zinc-950 to-black">
          {children}
        </div>
      </body>
    </html>
  );
}


