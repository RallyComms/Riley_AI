"use client";

import { useEffect, useState } from "react";
import { useUser } from "@clerk/nextjs";

export default function Greeting() {
  const { user, isLoaded } = useUser();
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  // Render placeholder until both mounted and Clerk is loaded
  if (!mounted || !isLoaded) {
    return <>there</>;
  }

  // After mount and load, render user's firstName or fallback to "there"
  return <>{user?.firstName ?? "there"}</>;
}
