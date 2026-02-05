import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "@app/styles/globals.css";
import { cn } from "@app/lib/utils";
import { Providers } from "./providers";
import { GlobalRileyOrb } from "@app/components/layout/GlobalRileyOrb";
import { GridBackground } from "@app/components/layout/GridBackground";
import { ClerkProvider } from "@clerk/nextjs";
import { dark } from "@clerk/themes";

const fontSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const fontMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

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
          "min-h-screen font-sans antialiased selection:bg-zinc-800",
          fontSans.variable,
          fontMono.variable
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
            {children}
            <GlobalRileyOrb />
          </Providers>
        </ClerkProvider>
      </body>
    </html>
  );
}

