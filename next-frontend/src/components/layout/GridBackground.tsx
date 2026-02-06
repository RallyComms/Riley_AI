"use client";

import React, { useState, useEffect, useMemo } from "react";
import { usePathname } from "next/navigation";
import { useSpring, useMotionValue } from "framer-motion";
import { motion } from "framer-motion";

/**
 * GridBackground - Tron Legacy Grid with Reactive Particle Swarm
 * 
 * Features:
 * - Sharp, high-contrast Tron-style SVG grid with glow
 * - Swarm of 60-80 reactive particles that respond to mouse
 * - Parallax physics for 3D depth effect
 * - Hardware-accelerated animations
 * - Particles only visible on home page
 */
interface Particle {
  id: number;
  initialX: number;
  initialY: number;
  size: number;
  color: "cyan" | "amber";
  pulseDuration: number;
  strength: number; // Parallax strength (0.1 to 0.5)
}

export function GridBackground() {
  const pathname = usePathname();
  const isHomePage = pathname === "/";
  const [isMounted, setIsMounted] = useState(false);

  // Prevent hydration mismatch - only render dynamic content after mount
  useEffect(() => {
    setIsMounted(true);
  }, []);

  // Mouse position tracking
  const mouseX = useMotionValue(50);
  const mouseY = useMotionValue(50);

  // Spring physics for smooth mouse following
  const springX = useSpring(mouseX, { damping: 30, stiffness: 100 });
  const springY = useSpring(mouseY, { damping: 30, stiffness: 100 });

  // Track mouse position
  useEffect(() => {
    if (!isHomePage) return;

    const handleMouseMove = (e: MouseEvent) => {
      // Normalize to percentage (0-100)
      const normalizedX = (e.clientX / window.innerWidth) * 100;
      const normalizedY = (e.clientY / window.innerHeight) * 100;
      
      mouseX.set(normalizedX);
      mouseY.set(normalizedY);
    };

    window.addEventListener("mousemove", handleMouseMove, { passive: true });
    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
    };
  }, [isHomePage, mouseX, mouseY]);

  // Generate particle swarm (60-80 particles)
  const particles = useMemo<Particle[]>(() => {
    if (!isHomePage) return [];
    
    const count = 70; // Number of particles
    const particlesArray: Particle[] = [];
    
    for (let i = 0; i < count; i++) {
      particlesArray.push({
        id: i,
        initialX: Math.random() * 100, // 0-100%
        initialY: Math.random() * 100, // 0-100%
        size: 2 + Math.random() * 2, // 2-4px
        color: Math.random() < 0.8 ? "cyan" : "amber", // 80% cyan, 20% amber
        pulseDuration: 2 + Math.random() * 3, // 2-5 seconds
        strength: 0.1 + Math.random() * 0.4, // 0.1 to 0.5 parallax strength
      });
    }
    
    return particlesArray;
  }, [isHomePage]);

  // Return simple background during SSR to prevent hydration mismatch
  if (!isMounted) {
    return (
      <div
        className="fixed inset-0 w-full h-full pointer-events-none bg-[#020617]"
        style={{ zIndex: -1 }}
      />
    );
  }

  return (
    <div
      className="fixed inset-0 w-full h-full pointer-events-none"
      style={{ zIndex: -1 }}
    >
      {/* Tron Legacy Grid - Sharp, High-Contrast */}
      <svg
        className="absolute inset-0 w-full h-full"
        xmlns="http://www.w3.org/2000/svg"
      >
        <defs>
          {/* Glow filter for grid lines */}
          <filter id="grid-glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="0.5" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          
          {/* Grid pattern */}
          <pattern
            id="tron-grid-pattern"
            x="0"
            y="0"
            width="60"
            height="60"
            patternUnits="userSpaceOnUse"
          >
            <path
              d="M 60 0 L 0 0 0 60"
              fill="none"
              stroke="rgba(56, 189, 248, 0.1)"
              strokeWidth="0.5"
              filter="url(#grid-glow)"
            />
          </pattern>
          
          {/* Secondary blurred layer for depth */}
          <pattern
            id="tron-grid-blur"
            x="0"
            y="0"
            width="60"
            height="60"
            patternUnits="userSpaceOnUse"
          >
            <path
              d="M 60 0 L 0 0 0 60"
              fill="none"
              stroke="rgba(56, 189, 248, 0.1)"
              strokeWidth="1"
            />
          </pattern>
        </defs>
        
        {/* Main grid layer */}
        <rect width="100%" height="100%" fill="url(#tron-grid-pattern)" />
        
        {/* Blurred depth layer */}
        <rect width="100%" height="100%" fill="url(#tron-grid-blur)" opacity="0.5" />
      </svg>

      {/* Reactive Particle Swarm (Home Page Only) */}
      {isHomePage && particles.map((particle) => (
        <ParticleOrb
          key={particle.id}
          particle={particle}
          springX={springX}
          springY={springY}
        />
      ))}
    </div>
  );
}

/**
 * Individual particle orb component with reactive physics
 */
interface ParticleOrbProps {
  particle: Particle;
  springX: any;
  springY: any;
}

function ParticleOrb({ particle, springX, springY }: ParticleOrbProps) {
  const [springXValue, setSpringXValue] = useState(50);
  const [springYValue, setSpringYValue] = useState(50);

  // Subscribe to spring values
  useEffect(() => {
    const unsubscribeX = springX.on("change", (latest: number) => {
      setSpringXValue(latest);
    });
    const unsubscribeY = springY.on("change", (latest: number) => {
      setSpringYValue(latest);
    });

    return () => {
      unsubscribeX();
      unsubscribeY();
    };
  }, [springX, springY]);

  // Calculate parallax offset
  const mouseOffsetX = (springXValue - 50) * particle.strength;
  const mouseOffsetY = (springYValue - 50) * particle.strength;

  const finalX = particle.initialX + mouseOffsetX;
  const finalY = particle.initialY + mouseOffsetY;

  const colorClass = particle.color === "cyan" 
    ? "bg-cyan-400" 
    : "bg-amber-400";
  
  const glowColor = particle.color === "cyan"
    ? "0 0 8px rgba(34, 211, 238, 0.6), 0 0 4px rgba(34, 211, 238, 0.4)"
    : "0 0 8px rgba(251, 191, 36, 0.6), 0 0 4px rgba(251, 191, 36, 0.4)";

  return (
    <motion.div
      className={`absolute rounded-full ${colorClass}`}
      style={{
        left: `${finalX}%`,
        top: `${finalY}%`,
        width: `${particle.size}px`,
        height: `${particle.size}px`,
        boxShadow: glowColor,
        transform: "translate(-50%, -50%)",
      }}
      animate={{
        opacity: [0.2, 0.8, 0.2],
      }}
      transition={{
        duration: particle.pulseDuration,
        repeat: Infinity,
        ease: "easeInOut",
      }}
    />
  );
}
