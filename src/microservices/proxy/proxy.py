from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError
import random
import os
import json

class ProxyHandler(BaseHTTPRequestHandler):
    # Конфигурация
    MONOLITH = os.getenv('MONOLITH_URL', 'http://monolith:8080')
    MOVIES_SERVICE = os.getenv('MOVIES_SERVICE_URL', 'http://movies-service:8081')
    EVENTS_SERVICE = os.getenv('EVENTS_SERVICE_URL', 'http://events-service:8082')
    MIGRATION_PERCENT = int(os.getenv('MOVIES_MIGRATION_PERCENT', '50'))
    GRADUAL_MIGRATION = os.getenv('GRADUAL_MIGRATION', 'true').lower() == 'true'
    
    def get_target_url(self, path):
        """Определяет целевой URL для запроса"""
        if path.startswith('/api/events'):
            return self.EVENTS_SERVICE
        
        if path.startswith('/api/movies'):
            if not self.GRADUAL_MIGRATION:
                return self.MONOLITH
            
            # Случайный выбор на основе процента миграции
            if random.randint(1, 100) <= self.MIGRATION_PERCENT:
                return self.MOVIES_SERVICE
            else:
                return self.MONOLITH
        
        return self.MONOLITH
    
    def do_GET(self):
        """Обрабатывает GET запросы"""
        target_url = self.get_target_url(self.path)
        full_url = f"{target_url}{self.path}"
        
        try:
            # Проксируем запрос
            req = Request(full_url, headers=dict(self.headers))
            with urlopen(req) as response:
                self.send_response(response.status)
                
                # Копируем заголовки
                for key, value in response.headers.items():
                    self.send_header(key, value)
                self.end_headers()
                
                # Копируем тело ответа
                self.wfile.write(response.read())
                
                # Лог для отладки
                print(f"GET {self.path} -> {target_url} (status: {response.status})")
                
        except URLError as e:
            self.send_error(502, f"Bad Gateway: {str(e)}")
    
    def do_POST(self):
        """Обрабатывает POST запросы"""
        target_url = self.get_target_url(self.path)
        full_url = f"{target_url}{self.path}"
        
        try:
            # Читаем тело запроса
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length) if content_length > 0 else None
            
            # Проксируем запрос
            req = Request(full_url, data=body, headers=dict(self.headers), method='POST')
            with urlopen(req) as response:
                self.send_response(response.status)
                
                # Копируем заголовки
                for key, value in response.headers.items():
                    self.send_header(key, value)
                self.end_headers()
                
                # Копируем тело ответа
                self.wfile.write(response.read())
                
                # Лог для отладки
                print(f"POST {self.path} -> {target_url} (status: {response.status})")
                
        except URLError as e:
            self.send_error(502, f"Bad Gateway: {str(e)}")
    
    def do_PUT(self):
        """Обрабатывает PUT запросы"""
        self.do_POST()  # Используем ту же логику
    
    def do_DELETE(self):
        """Обрабатывает DELETE запросы"""
        target_url = self.get_target_url(self.path)
        full_url = f"{target_url}{self.path}"
        
        try:
            req = Request(full_url, headers=dict(self.headers), method='DELETE')
            with urlopen(req) as response:
                self.send_response(response.status)
                for key, value in response.headers.items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(response.read())
                print(f"DELETE {self.path} -> {target_url}")
                
        except URLError as e:
            self.send_error(502, f"Bad Gateway: {str(e)}")

if __name__ == '__main__':
    port = int(os.getenv('PORT', '8000'))
    server = HTTPServer(('0.0.0.0', port), ProxyHandler)
    print(f'Proxy server running on port {port}')
    print(f'Movies migration: {ProxyHandler.MIGRATION_PERCENT}%')
    server.serve_forever()