import asyncio
import json
import os
import html
import hashlib
import secrets
import uuid
import time
import re
import base64
import sqlite3
import socket
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from collections import deque, defaultdict
from random import choice

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import Response, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn
import httpx
import logging
import psutil

try:
    import telebot
    from telebot.async_telebot import AsyncTeleBot
    from telebot import types
    TELEBOT_AVAILABLE = True
except ImportError:
    TELEBOT_AVAILABLE = False
    print("WARNING: Please install pyTelegramBotAPI to enable the Telegram Bot")

log_queue = deque(maxlen=150)

class QueueHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            log_queue.append(msg)
        except Exception:
            pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("mmd-Gateway")

q_handler = QueueHandler()
q_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(q_handler)
logging.getLogger("uvicorn.error").addHandler(q_handler)
logging.getLogger("uvicorn.access").addHandler(q_handler)

app = FastAPI(title="mmd Panel", docs_url=None, redoc_url=None)

# ── Glass Neon Theme (Dark Only) ────────────────────────────────────────
THEME = {
    "primary": "#8B5CF6",
    "secondary": "#06B6D4",
    "accent": "#22C55E",
    "background": "#0A0A0A",
    "glass": "rgba(15, 23, 42, 0.75)",
    "glow_purple": "0 0 30px #8B5CF6, 0 0 60px #8B5CF6",
    "glow_cyan": "0 0 30px #06B6D4, 0 0 60px #06B6D4",
    "glow_green": "0 0 30px #22C55E, 0 0 60px #22C55E",
    "border": "rgba(139, 92, 246, 0.3)"
}

# ── Live Particles ──────────────────────────────────────────────────────
class Particle:
    def __init__(self):
        self.x = 0
        self.y = 0
        self.vx = 0
        self.vy = 0
        self.size = 0
        self.color = ""

    def reset(self, w, h):
        self.x = random.uniform(0, w)
        self.y = random.uniform(0, h)
        self.vx = random.uniform(-1.2, 1.2)
        self.vy = random.uniform(-1.2, 1.2)
        self.size = random.uniform(1.5, 4)
        self.color = choice([THEME["primary"], THEME["secondary"], THEME["accent"]])

    def update(self, dt):
        self.x += self.vx * dt
        self.y += self.vy * dt
        if self.x < 0 or self.x > canvas.width: self.vx *= -1
        if self.y < 0 or self.y > canvas.height: self.vy *= -1

    def draw(self):
        ctx.fillStyle = self.color
        ctx.shadowBlur = 35
        ctx.shadowColor = self.color
        ctx.beginPath()
        ctx.arc(self.x, self.y, self.size, 0, Math.PI * 2)
        ctx.fill()

# ── PANEL HTML (تمام تغییرات) ───────────────────────────────────────────
PANEL_HTML = '''<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>mmd Panel • Glass Neon</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>
        :root {--primary: #8B5CF6; --secondary: #06B6D4; --glass: ''' + THEME["glass"] + '''; --glow: ''' + THEME["glow_purple"] + ''';}
        * {transition: all 0.4s cubic-bezier(0.23,1,0.32,1);}
        body {background:#0A0A0A; color:#E2E8F0; font-family:'Segoe UI',sans-serif;}
        .glass {background:var(--glass); backdrop-filter:blur(24px); border:1px solid ''' + THEME["border"] + '''; border-radius:28px; box-shadow:0 10px 40px rgba(0,0,0,0.5), var(--glow);}
        .glass:hover, button, .card {box-shadow:0 0 60px var(--primary), 0 0 120px var(--secondary); border:1px solid var(--secondary); transform:translateY(-6px) scale(1.03);}
        .neon-active {box-shadow:var(--glow), 0 0 40px #fff;}
        .bg-canvas {position:fixed; top:0; left:0; width:100%; height:100%; z-index:-2; pointer-events:none;}
        .flag {width:32px; height:22px; border-radius:4px; overflow:hidden;}
    </style>
</head>
<body class="min-h-screen overflow-hidden">
    <canvas id="bg-canvas" class="bg-canvas"></canvas>

    <div class="max-w-7xl mx-auto p-8">
        <!-- Left Sidebar - Half Width -->
        <div class="glass w-1/2 h-screen p-6 flex flex-col fixed left-0 top-0 rounded-r-3xl">
            <div class="flex items-center gap-4 mb-8">
                <div class="w-12 h-12 bg-gradient-to-br from-[#8B5CF6] to-[#06B6D4] rounded-3xl flex items-center justify-center text-white text-4xl shadow-[0_0_50px_#8B5CF6]">MMD</div>
                <div>
                    <h1 class="text-4xl font-bold bg-gradient-to-r from-white to-[#06B6D4] bg-clip-text text-transparent">mmd Panel</h1>
                    <p class="text-[#06B6D4] text-sm">Glass Neon Live • v1.2</p>
                </div>
            </div>

            <nav class="flex-1 space-y-2">
                <a onclick="switchTab(0)" class="glass p-5 rounded-2xl flex items-center gap-4 active">🏠 Dashboard</a>
                <a onclick="switchTab(1)" class="glass p-5 rounded-2xl flex items-center gap-4">👥 Users</a>
                <a onclick="switchTab(2)" class="glass p-5 rounded-2xl flex items-center gap-4">📊 Inbounds</a>
                <a onclick="switchTab(3)" class="glass p-5 rounded-2xl flex items-center gap-4">📈 Stats</a>
            </nav>
        </div>

        <!-- Main Content -->
        <div class="ml-[52%] p-8">
            <!-- Dashboard Header -->
            <div class="glass p-8 rounded-3xl mb-8">
                <h2 class="text-3xl font-bold">Welcome to mmd Panel</h2>
                <p class="text-[#6B7280] mt-2">Manage your VLESS & Trojan inbounds with live stats</p>
            </div>

            <!-- Online Users + Traffic Circles (Dashboard) -->
            <div class="grid grid-cols-2 gap-6 mb-8">
                <div class="glass p-8 rounded-3xl">
                    <h3 class="text-xl font-semibold mb-6">Users Online</h3>
                    <div class="text-7xl font-bold text-[#22C55E]" id="online-count">0</div>
                    <p class="text-[#6B7280]">Active connections right now</p>
                </div>
                <div class="glass p-8 rounded-3xl flex flex-col">
                    <h3 class="text-xl font-semibold mb-6">Traffic</h3>
                    <div class="flex-1 flex items-center justify-around" id="traffic-circles">
                        <!-- JS will fill -->
                    </div>
                </div>
            </div>

            <!-- Charts (Bar + Doughnut) -->
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div class="glass p-6 rounded-3xl">
                    <h3 class="text-xl font-semibold mb-4">Hourly Traffic</h3>
                    <canvas id="hourly-chart" class="h-80"></canvas>
                </div>
                <div class="glass p-6 rounded-3xl">
                    <h3 class="text-xl font-semibold mb-4">Server CPU & RAM</h3>
                    <div class="flex justify-around" id="cpu-doughnut"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let ctx, particles = [], onlineCount = 0;
        const canvas = document.createElement('canvas');
        canvas.id = 'bg-canvas';
        document.body.appendChild(canvas);

        function resizeCanvas() {
            canvas.width = window.innerWidth;
            canvas.height = window.innerHeight;
            particles = [];
            for (let i = 0; i < 150; i++) particles.push(new Particle());
        }

        function animate() {
            ctx.fillStyle = 'rgba(10,10,10,0.2)';
            ctx.fillRect(0,0,canvas.width,canvas.height);
            particles.forEach(p => { p.update(1); p.draw(); });
            requestAnimationFrame(animate);
        }

        // Fake live data (in real panel you replace with fetch)
        function updateLiveData() {
            onlineCount = Math.floor(Math.random() * 25);
            document.getElementById('online-count').textContent = onlineCount;
            // traffic circles
            // hourly chart
            // cpu doughnut
        }

        // Start
        resizeCanvas();
        ctx = canvas.getContext('2d');
        animate();
        setInterval(updateLiveData, 4000);
    </script>
</body>
</html>'''

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

@app.get("/panel", response_class=HTMLResponse)
async def panel_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
