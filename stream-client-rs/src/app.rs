use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::env;

// 请求结构体
#[derive(Debug, Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<Message>,
    temperature: f32,
    max_tokens: u32,
    stream: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    role: String,
    content: String,
}

// 非流式响应结构体
#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Debug, Deserialize)]
struct Choice {
    message: Message,
}

// 流式响应结构体 (SSE 格式)
#[derive(Debug, Deserialize)]
struct StreamChunk {
    choices: Vec<StreamChoice>,
}

#[derive(Debug, Deserialize)]
struct StreamChoice {
    delta: Delta,
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Delta {
    content: Option<String>,
}

// 从环境变量获取 API 基础 URL，如果未设置则使用默认值
fn get_api_base_url() -> String {
    env::var("API_BASE_URL")
        .unwrap_or_else(|_| "http://121.40.102.152:8080".to_string())
}

// #[tokio::main]
pub async fn run() -> Result<()> {
    // 从命令行参数获取 stream 模式，默认为 false
    let args: Vec<String> = env::args().collect();
    let use_stream = if args.len() > 1 {
        args[1] == "true"
    } else {
        false
    };

    println!("使用 {} 模式", if use_stream { "流式" } else { "非流式" });

    // 创建 HTTP 客户端
    let client = Client::new();

    // 构建请求体
    let request = ChatRequest {
        model: "chat".to_string(),
        messages: vec![
            Message {
                role: "system".to_string(),
                content: "You are a helpful assistant.".to_string(),
            },
            Message {
                role: "user".to_string(),
                content: "who are you?".to_string(),
            },
        ],
        temperature: 0.7,
        max_tokens: 1000,
        stream: use_stream,
    };

    if use_stream {
        // 流式模式
        handle_stream_response(client, request).await?;
    } else {
        // 非流式模式
        handle_normal_response(client, request).await?;
    }

    Ok(())
}

// 处理非流式响应
async fn handle_normal_response(client: Client, request: ChatRequest) -> Result<()> {
    println!("发送请求...");

    let base_url = get_api_base_url();
    let response = client
        .post(format!("{}/v1/chat/completions", base_url))
        .header("Content-Type", "application/json")
        .json(&request)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(anyhow!("请求失败: {}", response.status()));
    }

    let chat_response: ChatResponse = response.json().await?;

    if let Some(choice) = chat_response.choices.first() {
        println!("\n助手回复:\n{}", choice.message.content);
    }

    Ok(())
}

// 处理流式响应
async fn handle_stream_response(client: Client, request: ChatRequest) -> Result<()> {
    println!("发送流式请求...\n");

    let base_url = get_api_base_url();
    let response = client
        .post(format!("{}/v1/chat/completions", base_url))
        .header("Content-Type", "application/json")
        .json(&request)
        .send()
        .await?;

    if !response.status().is_success() {
        let error_text = response.text().await?;
        return Err(anyhow!("请求失败: {}", error_text));
    }

    // 获取响应流
    let mut stream = response.bytes_stream();

    let mut buffer = String::new();
    let mut full_content = String::new();

    println!("助手回复 (流式):");
    print!("> ");

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result?;
        let chunk_str = String::from_utf8_lossy(&chunk);

        buffer.push_str(&chunk_str);

        // 处理 SSE 格式的数据 (以 \n\n 分隔)
        while let Some(line_end) = buffer.find("\n\n") {
            let line = buffer[..line_end].trim().to_string();
            buffer = buffer[line_end + 2..].to_string();

            // 处理 "data: " 格式的行
            if let Some(data) = line.strip_prefix("data: ") {
                if data == "[DONE]" {
                    println!("\n\n流式传输完成");
                    break;
                }

                // 解析 JSON 数据
                match serde_json::from_str::<StreamChunk>(data) {
                    Ok(chunk_data) => {
                        for choice in chunk_data.choices {
                            if let Some(content) = choice.delta.content {
                                print!("{}", content);
                                full_content.push_str(&content);
                                // 刷新输出缓冲区，实现实时显示
                                std::io::Write::flush(&mut std::io::stdout())?;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("\n解析流式数据失败: {}", e);
                        eprintln!("原始数据: {}", data);
                    }
                }
            }
        }
    }

    println!("\n\n完整内容长度: {} 字符", full_content.len());
    Ok(())
}
