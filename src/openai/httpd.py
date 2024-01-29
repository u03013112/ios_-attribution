from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # 这将使您的Flask应用支持CORS

@app.route('/api/openai/chat/completions', methods=['POST'])
def handle_post_request():
    if request.method == 'OPTIONS':
        # 预检请求，直接返回200状态码
        return '', 200
    request_body = request.get_json()
    print("Received request body:", request_body)
    response = {'code': 0}
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3004)
