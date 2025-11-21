# bridge.py

import asyncio
import websockets
import json  # Ajouter cette ligne

WEBSOCKET_URL = "ws://localhost:8000/websocket"
TCP_IP = "localhost"
TCP_PORT = 9999

clients = []

async def handle_tcp_client(reader, writer):
    client_addr = writer.get_extra_info('peername')
    print(f" Spark connecté depuis {client_addr}!")
    clients.append(writer)
    
    try:
        while True:
            try:
                data = await asyncio.wait_for(reader.read(1), timeout=1.0)
                if not data:
                    break
            except asyncio.TimeoutError:
                continue
            except Exception:
                break
    except Exception as e:
        print(f" Erreur client TCP : {e}")
    finally:
        if writer in clients:
            clients.remove(writer)
        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass
        print(f" Spark déconnecté ({client_addr})")

async def tcp_server():
    server = await asyncio.start_server(handle_tcp_client, TCP_IP, TCP_PORT)
    print(f" Serveur TCP actif sur {TCP_IP}:{TCP_PORT}")
    async with server:
        await server.serve_forever()

async def forward_ws():
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL) as ws:
                print(f" Connecté au WebSocket : {WEBSOCKET_URL}")
                
                while True:
                    msg = await ws.recv()
                    
                    # DEBUG: Vérifier le contenu JSON
                    try:
                        data = json.loads(msg)
                        print(f" DEBUG: Nombre de champs reçus: {len(data)}")
                        print(f" DEBUG: Champs: {list(data.keys())}")
                    except:
                        print(f" DEBUG: Erreur parsing JSON")
                    
                    disconnected_clients = []
                    for writer in clients:
                        try:
                            writer.write((msg + "\n").encode("utf-8"))
                            await writer.drain()
                        except Exception as e:
                            print(f" Erreur lors de l'envoi à un client : {e}")
                            disconnected_clients.append(writer)
                    
                    for writer in disconnected_clients:
                        if writer in clients:
                            clients.remove(writer)
                            try:
                                writer.close()
                                await writer.wait_closed()
                            except:
                                pass
                    
                    if clients:
                        print(f"  Message envoyé à {len(clients)} client(s).")
                        
        except websockets.exceptions.ConnectionClosed:
            print(" WebSocket fermé, reconnexion dans 5 secondes...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f" Erreur WebSocket : {e}, reconnexion dans 5 secondes...")
            await asyncio.sleep(5)

async def main():
    await asyncio.gather(
        tcp_server(),
        forward_ws()
    )

if __name__ == "__main__":
    asyncio.run(main())
