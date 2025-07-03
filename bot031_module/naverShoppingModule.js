
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

// 상품 정보 크롤링 함수
async function crawlProductInfo(page) {
    console.log("상품 정보 크롤링 시작...");
    
    // 페이지가 완전히 로드될 때까지 대기
    await page.waitForTimeout(3000);
    
    // 페이지 스크롤하면서 상품 수집 (최대 40개)
    console.log("페이지 스크롤 중...");
    let totalProducts = 0;
    let scrollCount = 0;
    const maxScrolls = 10; // 최대 스크롤 횟수
    
    while (totalProducts < 40 && scrollCount < maxScrolls) {
        // 현재 상품 개수 확인
        const currentCount = await page.evaluate(() => {
            const adProducts = document.querySelectorAll('.adProduct_item__T7utB').length;
            const normalProducts = document.querySelectorAll('.product_item__KQayS').length;
            
            // 해외상품 개수 확인
            const overseasSelectors = [
                '.overseas_item__',
                '.global_item__',
                '.international_item__',
                '.foreign_item__',
                '[class*="overseas"]',
                '[class*="global"]',
                '[class*="international"]',
                '[class*="foreign"]'
            ];
            
            let overseasCount = 0;
            overseasSelectors.forEach(selector => {
                try {
                    const found = document.querySelectorAll(selector);
                    overseasCount += found.length;
                } catch (e) {
                    // 셀렉터가 유효하지 않으면 무시
                }
            });
            
            return adProducts + normalProducts + overseasCount;
        });
        
        console.log(`현재 ${currentCount}개의 상품 발견`);
        
        if (currentCount >= 40) {
            totalProducts = currentCount;
            break;
        }
        
        // 페이지 끝까지 스크롤
        await page.evaluate(() => {
            window.scrollTo(0, document.body.scrollHeight);
        });
        
        // 스크롤 후 로딩 대기
        await page.waitForTimeout(2000);
        
        // 새로운 상품이 로드되었는지 확인
        const newCount = await page.evaluate(() => {
            const adProducts = document.querySelectorAll('.adProduct_item__T7utB').length;
            const normalProducts = document.querySelectorAll('.product_item__KQayS').length;
            
            // 해외상품 개수 확인
            const overseasSelectors = [
                '.overseas_item__',
                '.global_item__',
                '.international_item__',
                '.foreign_item__',
                '[class*="overseas"]',
                '[class*="global"]',
                '[class*="international"]',
                '[class*="foreign"]'
            ];
            
            let overseasCount = 0;
            overseasSelectors.forEach(selector => {
                try {
                    const found = document.querySelectorAll(selector);
                    overseasCount += found.length;
                } catch (e) {
                    // 셀렉터가 유효하지 않으면 무시
                }
            });
            
            return adProducts + normalProducts + overseasCount;
        });
        
        if (newCount === currentCount) {
            // 새로운 상품이 로드되지 않았으면 더 이상 스크롤할 필요 없음
            totalProducts = newCount;
            break;
        }
        
        totalProducts = newCount;
        scrollCount++;
    }
    
    console.log(`총 ${totalProducts}개의 상품을 수집했습니다.`);
    
    // 상품 정보 추출
    const products = await page.evaluate(() => {
        const productList = [];
        let productIndex = 1;
        
        // 모든 상품 요소를 실제 표시 순서대로 수집
        const allProductElements = document.querySelectorAll('.adProduct_item__T7utB, .product_item__KQayS');
        
        allProductElements.forEach((product) => {
            try {
                let type = '일반';
                let title = '상품명 없음';
                let rating = '평점 없음';
                let reviewCount = '후기 없음';
                let price = '가격 없음';
                let mall = '확인필요';
                let link = '링크 없음';
                let isOverseas = false;
                
                // 광고 상품인지 확인
                if (product.classList.contains('adProduct_item__T7utB')) {
                    type = '광고';
                    
                    // 상품명
                    const titleElement = product.querySelector('.adProduct_title__fsQU6 .adProduct_link__hNwpz');
                    title = titleElement ? titleElement.getAttribute('title') || titleElement.textContent.trim() : '상품명 없음';
                    
                    // 평점
                    const ratingElement = product.querySelector('.adProduct_rating__vk1YN');
                    rating = ratingElement ? ratingElement.textContent.trim() : '평점 없음';
                    
                    // 후기수
                    const reviewElement = product.querySelector('.adProduct_count__J5x57');
                    reviewCount = reviewElement ? reviewElement.textContent.trim() : '후기 없음';
                    
                    // 가격
                    const priceElement = product.querySelector('.adProduct_price__aI_aG .price_num__Y66T7 em');
                    price = priceElement ? priceElement.textContent.trim() : '가격 없음';
                    
                    // 판매처
                    const mallElement = product.querySelector('.adProduct_mall__grJaU');
                    if (mallElement && mallElement.textContent.trim()) {
                        mall = mallElement.textContent.trim();
                        // 해외상품 키워드 확인
                        const overseasKeywords = ['구매대행', '해외직구', '글로벌', '해외배송', '직구', '대행', '해외'];
                        if (overseasKeywords.some(keyword => mall.includes(keyword))) {
                            isOverseas = true;
                        }
                    }
                    
                    // 상품링크
                    const linkElement = product.querySelector('.adProduct_link__hNwpz');
                    link = linkElement ? linkElement.href : '링크 없음';
                    
                } else {
                    // 일반 상품
                    type = '일반';
                    
                    // 상품명
                    const titleElement = product.querySelector('.product_title__ljFM_ .product_link__aFnaq');
                    title = titleElement ? titleElement.getAttribute('title') || titleElement.textContent.trim() : '상품명 없음';
                    
                    // 평점
                    const ratingElement = product.querySelector('.product_grade__O_5f5');
                    rating = ratingElement ? ratingElement.textContent.trim() : '평점 없음';
                    
                    // 후기수
                    const reviewElement = product.querySelector('.product_etc__Z7jnS .product_num__WuH26');
                    reviewCount = reviewElement ? reviewElement.textContent.trim() : '후기 없음';
                    
                    // 가격
                    const priceElement = product.querySelector('.product_price__ozt5Q .price_num__Y66T7 em');
                    price = priceElement ? priceElement.textContent.trim() : '가격 없음';
                    
                    // 판매처 처리
                    const mallElement = product.querySelector('.product_mall__0cRyd');
                    if (mallElement && mallElement.textContent.trim()) {
                        mall = mallElement.textContent.trim();
                        // 해외상품 키워드 확인
                        const overseasKeywords = ['구매대행', '해외직구', '글로벌', '해외배송', '직구', '대행', '해외'];
                        if (overseasKeywords.some(keyword => mall.includes(keyword))) {
                            isOverseas = true;
                        }
                    } else {
                        // 쇼핑몰별 최저가 확인
                        const catalogElement = product.querySelector('.product_catalog__KATO4');
                        if (catalogElement && catalogElement.textContent.trim()) {
                            mall = '쇼핑몰별 최저가';
                        } else {
                            // 브랜드 카탈로그 확인
                            const brandCatalogElement = product.querySelector('.product_brand__QfHNn');
                            if (brandCatalogElement && brandCatalogElement.textContent.trim()) {
                                mall = '카탈로그';
                            } else {
                                // 다른 판매처 요소들 확인
                                const otherMallElement = product.querySelector('.product_mall_title__sJPEp a');
                                if (otherMallElement && otherMallElement.textContent.trim()) {
                                    mall = otherMallElement.textContent.trim();
                                    // 해외상품 키워드 확인
                                    const overseasKeywords = ['구매대행', '해외직구', '글로벌', '해외배송', '직구', '대행', '해외'];
                                    if (overseasKeywords.some(keyword => mall.includes(keyword))) {
                                        isOverseas = true;
                                    }
                                }
                            }
                        }
                    }
                    
                    // 상품링크
                    const linkElement = product.querySelector('.product_link__aFnaq');
                    link = linkElement ? linkElement.href : '링크 없음';
                }
                
                // 해외상품인 경우 타입 변경
                if (isOverseas) {
                    type = '해외상품';
                }
                
                productList.push({
                    type,
                    title,
                    rating,
                    reviewCount,
                    price,
                    mall,
                    link,
                    index: productIndex++
                });
            } catch (error) {
                console.log('상품 정보 추출 중 오류:', error);
            }
        });
        
        return productList;
    });
    
    return products;
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
                '--disable-default-apps',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-background-networking',
                '--start-maximized',
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

        // 상품 정보 크롤링
        const products = await crawlProductInfo(page);
        
        // 결과 출력
        console.log("\n=== 크롤링 결과 ===");
        console.log(`총 ${products.length}개의 상품을 찾았습니다.\n`);
        
        products.forEach((product, index) => {
            console.log(`[${product.index}] ${product.type}`);
            console.log(`상품명: ${product.title}`);
            console.log(`평점: ${product.rating}`);
            console.log(`후기수: ${product.reviewCount}`);
            console.log(`가격: ${product.price}원`);
            console.log(`판매처: ${product.mall}`);
            console.log(`링크: ${product.link}`);
            console.log('─'.repeat(80));
        });

        // 통계 계산 및 출력
        console.log("\n=== 검색 결과 통계 ===");
        
        // 1. 검색된 상품 수
        const totalProducts = products.length;
        console.log(`검색된 상품 수: ${totalProducts}개`);
        
        // 2. 평균가격 계산 (상위 5개와 하위 5개 제외)
        const priceData = products
            .map(product => {
                // 가격에서 숫자만 추출
                const priceStr = product.price.replace(/[^\d]/g, '');
                return parseInt(priceStr) || 0;
            })
            .filter(price => price > 0) // 유효한 가격만 필터링
            .sort((a, b) => a - b); // 오름차순 정렬
        
        let averagePrice = 0;
        if (priceData.length > 10) {
            // 상위 5개와 하위 5개를 제외한 가격들
            const filteredPrices = priceData.slice(5, -5);
            const sum = filteredPrices.reduce((acc, price) => acc + price, 0);
            averagePrice = Math.round(sum / filteredPrices.length);
            console.log(`평균가격 (상위/하위 5개 제외): ${averagePrice.toLocaleString()}원`);
        } else {
            // 상품이 10개 이하인 경우 전체 평균
            const sum = priceData.reduce((acc, price) => acc + price, 0);
            averagePrice = Math.round(sum / priceData.length);
            console.log(`평균가격: ${averagePrice.toLocaleString()}원`);
        }
        
        // 3. 광고상품 수
        const adProducts = products.filter(product => product.type === '광고').length;
        console.log(`광고상품 수: ${adProducts}개`);
        
        // 4. 해외상품 수 및 비율
        const overseasProducts = products.filter(product => product.type === '해외상품').length;
        const overseasRatio = totalProducts > 0 ? ((overseasProducts / totalProducts) * 100).toFixed(1) : 0;
        console.log(`해외상품 수: ${overseasProducts}개 (${overseasRatio}%)`);
        
        // 5. 상위 12개 중 리뷰 100개 미만인 상품 수
        const top12Products = products.slice(0, 12);
        const lowReviewProducts = top12Products.filter(product => {
            // 리뷰 수에서 숫자만 추출
            const reviewStr = product.reviewCount.replace(/[^\d]/g, '');
            const reviewCount = parseInt(reviewStr) || 0;
            return reviewCount < 100;
        }).length;
        console.log(`상위 12개 중 리뷰 100개 미만: ${lowReviewProducts}개`);
        
        console.log('─'.repeat(80));

        console.log("=== 쇼핑 탭 클릭 완료 ===");
        console.log("프로그램 실행 완료");
        console.log("크롬창이 열려있습니다. 수동으로 닫아주세요.");

    } catch (error) {
        console.error("프로그램 실행 중 오류 발생:", error);
    } finally {
        rl.close();
        // 브라우저를 열어둠 (수동으로 닫기)
        console.log("프로그램 종료 - 브라우저는 수동으로 닫아주세요.");
    }
}

// 프로그램 실행
main().catch(console.error); 