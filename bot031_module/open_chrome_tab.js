/**
 * STEP 1-1. 단일 키워드 
 * - 현재 검색어 상위노출 중인 블로거를 추출합니다. (단일 키워드)
 * - Puppeteer 버전
 */

const puppeteer = require('puppeteer');
const fs = require('fs').promises;
const path = require('path');
const readline = require('readline');
require('dotenv').config();

console.log("프로그램 시작...");

// 검색어와 글 수를 터미널에서 입력받기
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

async function getUserInput(prompt) {
    return new Promise((resolve) => {
        rl.question(prompt, (answer) => {
            resolve(answer.trim());
        });
    });
}

async function main() {
    let browser;
    try {
        const searchQuery = await getUserInput('검색어를 입력하세요: ');
        const numOfPosts = 15; // 글 수를 15로 고정
        console.log(`검색어 '${searchQuery}' 입력 완료`);

        // Puppeteer 브라우저 설정
        console.log("Chrome 브라우저 설정 중...");
        browser = await puppeteer.launch({
            headless: false, // 헤드리스 모드 비활성화
            defaultViewport: null, // 기본 뷰포트 비활성화
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            ]
        });

        console.log("Chrome 브라우저 설정 완료");

        // 새 페이지 생성 (about:blank 방지)
        const page = await browser.newPage();
        
        // 사용자 에이전트 설정
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3');

        // 네이버로 바로 이동 (about:blank 방지)
        console.log("네이버 메인 페이지로 이동 중...");
        await page.goto("https://www.naver.com", { 
            waitUntil: 'networkidle2',
            timeout: 30000 
        });
        
        // 페이지가 완전히 로드될 때까지 대기
        await page.waitForSelector('#query', { timeout: 10000 });
        await page.waitForTimeout(1000);
        console.log("네이버 메인 페이지 로드 완료");

        console.log(`검색어 '${searchQuery}' 입력 중...`);
        await page.type('#query', searchQuery);
        await page.click('#search-btn');
        await page.waitForTimeout(1000);
        console.log("검색 완료");

        // '쇼핑' 탭 클릭 (기존 탭에서 이동)
        console.log("쇼핑 페이지로 이동 중...");
        await page.goto("https://search.shopping.naver.com/search/all?query=" + encodeURIComponent(searchQuery), { 
            waitUntil: 'networkidle2',
            timeout: 30000 
        });
        await page.waitForTimeout(3000); // 페이지 로드 대기

        console.log("쇼핑 페이지 이동 완료");

        // 새 탭이 생겼는지 확인
        console.log("새 탭 생성 여부 확인 중...");
        const pages = await browser.pages();
        const newTabCount = pages.length;
        console.log(`현재 브라우저 탭 수: ${newTabCount}`);
        
        let newTabCreated = false;
        let captchaPage = null;
        
        // 새 탭이 생겼는지 확인 (기본적으로 1개 탭이 있으므로 2개 이상이면 새 탭이 생긴 것)
        if (newTabCount > 1) {
            newTabCreated = true;
            console.log("새 탭이 생성되었습니다!");
            
            // 새로 생성된 탭 찾기 (마지막 탭이 새로 생성된 탭)
            captchaPage = pages[pages.length - 1];
            console.log("새로 생성된 탭에서 CAPTCHA 확인을 진행합니다.");
            
            // 새 탭으로 포커스 이동
            await captchaPage.bringToFront();
        } else {
            console.log("새 탭이 생성되지 않았습니다. 현재 탭에서 계속 진행합니다.");
            captchaPage = page; // 현재 페이지 사용
        }

        console.log("CAPTCHA 확인 중...");

        // 페이지 소스를 'page_source.txt'로 저장
        const pageSource = await captchaPage.content();
        await fs.writeFile('page_source.txt', pageSource, 'utf-8');
        console.log("페이지 소스 저장 완료");

        // CAPTCHA가 나타날 때까지 대기
        console.log("CAPTCHA 로딩 대기 중...");
        await captchaPage.waitForTimeout(5000); // CAPTCHA 로딩 대기

        // 저장된 페이지 소스에서 CAPTCHA 관련 HTML 찾기
        console.log("저장된 페이지 소스에서 CAPTCHA 관련 HTML 검색...");
        try {
            const pageContent = await fs.readFile('page_source.txt', 'utf-8');
            
            // CAPTCHA 관련 키워드로 검색
            const captchaKeywords = ['captcha', 'rcpt', '캡차', 'security', 'verification'];
            for (const keyword of captchaKeywords) {
                if (pageContent.toLowerCase().includes(keyword.toLowerCase())) {
                    console.log(`'${keyword}' 키워드 발견!`);
                    // 해당 키워드 주변 텍스트 출력 (100자 전후)
                    const index = pageContent.toLowerCase().indexOf(keyword.toLowerCase());
                    if (index !== -1) {
                        const start = Math.max(0, index - 100);
                        const end = Math.min(pageContent.length, index + 100);
                        console.log(`주변 HTML: ${pageContent.substring(start, end)}`);
                    }
                }
            }
        } catch (e) {
            console.log(`페이지 소스 읽기 오류: ${e}`);
        }

        // OpenAI API 키 설정
        const openaiApiKey = process.env.OPENAI_API_KEY;

        // CAPTCHA 답변 얻기 함수
        async function getCaptchaAnswer(imagePath, question, apiKey) {
            console.log("\n=== GPT-4o CAPTCHA 해결 시작 ===");
            console.log(`질문: ${question}`);
            
            // 이미지를 base64로 인코딩
            const imageBuffer = await fs.readFile(imagePath);
            const encodedImage = imageBuffer.toString('base64');
            
            const headers = {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${apiKey}`
            };
            
            const payload = {
                "model": "gpt-4o",
                "messages": [
                    {
                        "role": "system",
                        "content": "당신은 이미지를 정확하게 해석하는 AI 도우미입니다. 이미지에 표시된 내용을 보고 질문에 대한 정확한 답변만 숫자 혹은 단어로 제공해주세요."
                    },
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": `다음 이미지를 보고 질문에 답해주세요. 질문: ${question}. 필요한 내용만 정확히 답변해주세요.`},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": `data:image/png;base64,${encodedImage}`
                                }
                            }
                        ]
                    }
                ],
                "max_tokens": 100
            };
            
            console.log("=== GPT 프롬프트 ===");
            console.log(`시스템 메시지: ${payload.messages[0].content}`);
            console.log(`사용자 메시지: ${payload.messages[1].content[0].text}`);
            console.log(`이미지 포함: ${encodedImage.length} 문자 (base64)`);
            console.log("=== API 요청 전송 중... ===");
            
            const axios = require('axios');
            const response = await axios.post("https://api.openai.com/v1/chat/completions", payload, { headers });
            const result = response.data;
            
            if (result.choices) {
                const answer = result.choices[0].message.content.trim();
                
                console.log("=== GPT 응답 ===");
                console.log(`원본 응답: '${answer}'`);
                
                // 토큰 사용량 계산 및 출력
                if (result.usage) {
                    // 토큰 사용량 계산
                    const promptTokens = result.usage.prompt_tokens;
                    const completionTokens = result.usage.completion_tokens;
                    const totalTokens = result.usage.total_tokens;
                    
                    // 토큰당 가격 (달러) - GPT-4o 가격
                    const promptPricePerToken = 0.00001; // $0.01 / 1K tokens
                    const completionPricePerToken = 0.00003; // $0.03 / 1K tokens
                    
                    // 달러 비용 계산
                    const promptCostUsd = promptTokens * promptPricePerToken;
                    const completionCostUsd = completionTokens * completionPricePerToken;
                    const totalCostUsd = promptCostUsd + completionCostUsd;
                    
                    // 한화로 환산 (환율: 1 USD = 약 1,350 KRW로 가정)
                    const exchangeRate = 1350;
                    const totalCostKrw = totalCostUsd * exchangeRate;
                    
                    console.log("\n=== GPT-4o 토큰 사용량 ===");
                    console.log(`프롬프트 토큰: ${promptTokens}`);
                    console.log(`응답 토큰: ${completionTokens}`);
                    console.log(`총 토큰: ${totalTokens}`);
                    console.log(`예상 비용: $${totalCostUsd.toFixed(6)} (약 ${totalCostKrw.toFixed(2)}원)`);
                }
                
                // 숫자만 추출
                const numericAnswer = answer.replace(/\D/g, '');
                const finalAnswer = numericAnswer || answer;
                
                console.log("=== 최종 CAPTCHA 답변 ===");
                console.log(`추출된 답변: '${finalAnswer}'`);
                console.log("=== GPT-4o CAPTCHA 해결 완료 ===\n");
                
                return finalAnswer;
            } else {
                console.log("=== API 응답 오류 ===");
                console.log("API 응답 오류:", result);
                console.log("기본값 '3' 반환\n");
                return "3"; // 오류 시 기본값
            }
        }

        // CAPTCHA 이미지 경로 설정
        const captchaImagePath = 'captcha_image.png';
        let captchaQuestion = '이미지에 표시된 숫자를 입력하세요.'; // 기본값

        console.log(`CAPTCHA 이미지 경로 설정: ${captchaImagePath}`);

        // 웹페이지에서 CAPTCHA 질문 찾기
        console.log("웹페이지에서 CAPTCHA 질문 검색 중...");
        try {
            // CAPTCHA 질문을 찾기 위한 셀렉터들
            const questionSelectors = [
                'p.captcha_message',
                'p#rcpt_info',
                'div.captcha_message',
                'div#rcpt_info',
                'span.captcha_message',
                'span#rcpt_info',
                'label[for*="captcha"]',
                'label[for*="rcpt"]',
                '.captcha_question',
                '.rcpt_question',
                'span[class*="question"]',
                'div[class*="question"]',
                'p[class*="question"]'
            ];
            
            let captchaQuestionFound = false;
            for (const selector of questionSelectors) {
                try {
                    const elements = await captchaPage.$$(selector);
                    for (const element of elements) {
                        const text = await captchaPage.evaluate(el => el.textContent, element);
                        if (text && text.trim().length > 5) { // 의미있는 텍스트가 있는지 확인
                            captchaQuestion = text.trim();
                            captchaQuestionFound = true;
                            console.log(`CAPTCHA 질문 발견: '${captchaQuestion}' (셀렉터: ${selector})`);
                            break;
                        }
                    }
                    if (captchaQuestionFound) break;
                } catch (e) {
                    continue;
                }
            }
            
            if (!captchaQuestionFound) {
                console.log("웹페이지에서 CAPTCHA 질문을 찾을 수 없어 기본값 사용");
                console.log(`기본 질문: '${captchaQuestion}'`);
            }
        } catch (e) {
            console.log(`CAPTCHA 질문 검색 중 오류: ${e}`);
            console.log(`기본 질문 사용: '${captchaQuestion}'`);
        }

        // 페이지 소스에서 CAPTCHA 관련 요소 확인
        console.log("페이지 소스에서 CAPTCHA 관련 요소 검색 중...");
        const currentPageSource = await captchaPage.content();
        if (currentPageSource.toLowerCase().includes('captcha') || currentPageSource.toLowerCase().includes('rcpt')) {
            console.log("페이지에 CAPTCHA 관련 내용이 있습니다.");
        } else {
            console.log("페이지에 CAPTCHA 관련 내용이 없습니다.");
        }

        // 모든 img 태그 찾기
        const allImages = await captchaPage.$$('img');
        console.log(`페이지에서 발견된 이미지 요소 수: ${allImages.length}`);

        for (let i = 0; i < allImages.length; i++) {
            try {
                const img = allImages[i];
                const imgInfo = await captchaPage.evaluate(el => ({
                    id: el.id || '',
                    className: el.className || '',
                    alt: el.alt || '',
                    src: el.src || ''
                }), img);
                console.log(`이미지 ${i+1}: id='${imgInfo.id}', class='${imgInfo.className}', alt='${imgInfo.alt}', src='${imgInfo.src.substring(0, 100)}...'`);
            } catch (e) {
                continue;
            }
        }

        // CAPTCHA 이미지 저장
        let base64ImageData = null; // 기본값 설정
        let captchaFound = false;

        console.log("JavaScript를 사용하여 CAPTCHA 이미지 데이터 추출 중...");

        // JavaScript를 사용하여 CAPTCHA 이미지 데이터 추출
        const jsScript = `
        function getImageAsBase64(imgElement) {
            const canvas = document.createElement('canvas');
            const ctx = canvas.getContext('2d');
            
            // 이미지 크기 설정
            canvas.width = imgElement.naturalWidth || imgElement.width;
            canvas.height = imgElement.naturalHeight || imgElement.height;
            
            // 이미지를 캔버스에 그리기
            ctx.drawImage(imgElement, 0, 0);
            
            // base64로 변환
            return canvas.toDataURL('image/png').split(',')[1];
        }

        // CAPTCHA 이미지 찾기 - 확실한 하나의 셀렉터만 사용
        let captchaImg = document.querySelector('img#rcpt_img');

        if (captchaImg) {
            return {
                found: true,
                base64: getImageAsBase64(captchaImg),
                src: captchaImg.src,
                alt: captchaImg.alt,
                id: captchaImg.id,
                className: captchaImg.className
            };
        } else {
            return {
                found: false,
                message: "CAPTCHA 이미지를 찾을 수 없습니다."
            };
        }
        `;

        try {
            console.log("JavaScript 실행 중...");
            const result = await captchaPage.evaluate(jsScript);
            
            if (result.found) {
                captchaFound = true;
                base64ImageData = result.base64;
                console.log("CAPTCHA 이미지 발견!");
                
                // src를 50자로 제한
                let srcInfo = result.src || 'N/A';
                if (srcInfo.length > 50) {
                    srcInfo = srcInfo.substring(0, 50) + '...';
                }
                
                console.log(`이미지 정보: src='${srcInfo}', alt='${result.alt || 'N/A'}', id='${result.id || 'N/A'}', class='${result.className || 'N/A'}'`);
                console.log(`Base64 데이터 길이: ${base64ImageData.length} 문자`);
                
                // base64 데이터를 파일로 저장 (선택사항)
                if (base64ImageData) {
                    const imageBuffer = Buffer.from(base64ImageData, 'base64');
                    await fs.writeFile(captchaImagePath, imageBuffer);
                    console.log(`CAPTCHA 이미지를 ${captchaImagePath}에 저장했습니다.`);
                }
            } else {
                console.log(`JavaScript 실행 결과: ${result.message || '알 수 없는 오류'}`);
            }
        } catch (e) {
            console.log(`JavaScript 실행 중 오류 발생: ${e}`);
            console.log("CAPTCHA 이미지 추출에 실패했습니다.");
            captchaFound = false;
            base64ImageData = null;
        }

        // 새 탭이 생성되었고 CAPTCHA가 발견된 경우에만 CAPTCHA 로직 실행
        if (newTabCreated && base64ImageData) {
            console.log("\n=== 새 탭에서 CAPTCHA 답변 입력 및 제출 시작 ===");
            const captchaAnswer = await getCaptchaAnswer(captchaImagePath, captchaQuestion, openaiApiKey);

            // CAPTCHA 입력 필드 찾기 및 입력
            console.log("CAPTCHA 입력 필드 검색 중...");
            const inputSelectors = [
                'input[type="text"]',
                'input[name*="captcha"]',
                'input[name*="rcpt"]',
                'input[id*="captcha"]',
                'input[id*="rcpt"]',
                'input[class*="captcha"]',
                'input[class*="rcpt"]',
                'input'
            ];
            
            let captchaInput = null;
            for (const selector of inputSelectors) {
                try {
                    captchaInput = await captchaPage.$(selector);
                    if (captchaInput) {
                        console.log(`CAPTCHA 입력 필드 발견 (셀렉터: ${selector})`);
                        break;
                    }
                } catch (e) {
                    continue;
                }
            }
            
            if (captchaInput) {
                console.log(`CAPTCHA 답변 '${captchaAnswer}' 입력 중...`);
                await captchaInput.click({ clickCount: 3 }); // 기존 내용 선택
                await captchaInput.type(captchaAnswer);
                console.log("CAPTCHA 답변 입력 완료");
            } else {
                console.log("CAPTCHA 입력 필드를 찾을 수 없습니다!");
            }

            // CAPTCHA 제출 버튼 찾기 및 클릭
            console.log("CAPTCHA 제출 버튼 검색 중...");
            const buttonSelectors = [
                'button#cpt_confirm',
                'button.btn_login',
                'button[type="submit"]',
                'button[class*="btn_login"]',
                'button[id*="cpt"]',
                'button[id*="confirm"]',
                'button[class*="submit"]',
                'button'
            ];
            
            let submitButton = null;
            for (const selector of buttonSelectors) {
                try {
                    submitButton = await captchaPage.$(selector);
                    if (submitButton) {
                        console.log(`CAPTCHA 제출 버튼 발견 (셀렉터: ${selector})`);
                        break;
                    }
                } catch (e) {
                    continue;
                }
            }
            
            if (submitButton) {
                console.log("CAPTCHA 제출 버튼 클릭 중...");
                await submitButton.click();
                console.log("CAPTCHA 제출 완료");
            } else {
                console.log("CAPTCHA 제출 버튼을 찾을 수 없습니다!");
            }

            console.log("페이지 로드 대기 중... (5초)");
            await captchaPage.waitForTimeout(5000); // 페이지 로드 대기
            console.log("=== 새 탭에서 CAPTCHA 답변 입력 및 제출 완료 ===\n");

            // CAPTCHA 완료 후 새 탭 닫기
            console.log("CAPTCHA 완료 후 새 탭 닫기...");
            await captchaPage.close();
            console.log("새 탭이 닫혔습니다.");

            // 원래 탭으로 돌아가기
            console.log("원래 탭으로 돌아가기...");
            const remainingPages = await browser.pages();
            if (remainingPages.length > 0) {
                await remainingPages[0].bringToFront();
                console.log("원래 탭으로 돌아왔습니다.");
            }

            // 쇼핑 페이지 새로고침 또는 재이동
            console.log("쇼핑 페이지 새로고침 중...");
            try {
                await page.reload({ waitUntil: 'networkidle2' });
                console.log("쇼핑 페이지 새로고침 완료");
                await page.waitForTimeout(3000); // 페이지 로드 대기
            } catch (e) {
                console.log(`쇼핑 페이지 새로고침 중 오류 발생: ${e}`);
            }
        } else if (!newTabCreated) {
            console.log("새 탭이 생성되지 않았으므로 CAPTCHA 로직을 건너뜁니다.");
        } else if (!base64ImageData) {
            console.log("CAPTCHA 이미지를 찾을 수 없으므로 CAPTCHA 로직을 건너뜁니다.");
        }

        // 브라우저 종료를 하지 않고 크롬 창을 열어둠
        // await browser.close();

        console.log("프로그램 실행 완료");
        console.log("30초 대기 중...");
        await page.waitForTimeout(30000);
        console.log("프로그램 종료");

    } catch (error) {
        console.error("프로그램 실행 중 오류 발생:", error);
    } finally {
        rl.close();
        if (browser) {
            // await browser.close();
        }
    }
}

// 프로그램 실행
main().catch(console.error); 