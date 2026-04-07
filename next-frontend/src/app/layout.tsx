import type { Metadata } from "next";
import "@app/styles/globals.css";
import { cn } from "@app/lib/utils";
import { Providers } from "./providers";
import { GridBackground } from "@app/components/layout/GridBackground";
import { GlobalTopNav } from "@app/components/layout/GlobalTopNav";
import { ClerkProvider } from "@clerk/nextjs";
import { dark } from "@clerk/themes";

export const metadata: Metadata = {
  title: "RILEY | Collaborative Intelligence",
  description: "Secure Advocacy Operations Platform",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body
        className={cn(
          "min-h-screen font-sans antialiased selection:bg-zinc-800"
        )}
      >
        <GridBackground />
        <ClerkProvider
          appearance={{
            baseTheme: dark,
            variables: {
              colorPrimary: "#fbbf24", // amber-400
              colorBackground: "#0f172a", // Tron Blue (Slate 900)
            },
          }}
        >
          <Providers>
            <GlobalTopNav />
            {children}
          </Providers>
        </ClerkProvider>
      </body>
    </html>
  );
}

