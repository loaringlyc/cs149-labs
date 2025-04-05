#include <math.h>
#include <stdio.h>
#include <immintrin.h>

#define VECTOR_WIDTH 8

void sqrtAvx2(int N,
    float initialGuess,
    float values[],
    float output[])
{
    __m256 guess, target, error, cmpResult;
    int mask;
    static const float kThreshold = 0.00001f;
    int aligned_n = (N / 8) * 8;

    for(int i = 0; i<aligned_n; i+=VECTOR_WIDTH){
        target = _mm256_loadu_ps(values+i);
        // target = _mm256_load_ps(values+i);
        guess = _mm256_set1_ps(initialGuess);

        error = _mm256_sub_ps(
            _mm256_mul_ps(_mm256_mul_ps(guess, guess), target),
            _mm256_set1_ps(1.0f)
        );
        error = _mm256_andnot_ps(_mm256_set1_ps(-0.0f), error); // 很巧妙的绝对值方法
        cmpResult = _mm256_cmp_ps(error, _mm256_set1_ps(kThreshold), _CMP_GT_OQ);
        mask = _mm256_movemask_ps(cmpResult);
        while(mask){
            guess = _mm256_mul_ps(
                _mm256_sub_ps(
                    _mm256_mul_ps(_mm256_set1_ps(3.0f), guess),
                    _mm256_mul_ps(target, _mm256_mul_ps(guess, _mm256_mul_ps(guess, guess)))
                ),
                _mm256_set1_ps(0.5f)
            );

            error = _mm256_sub_ps(
                _mm256_mul_ps(_mm256_mul_ps(guess, guess), target),
                _mm256_set1_ps(1.0f)
            );
            error = _mm256_andnot_ps(_mm256_set1_ps(-0.0f), error);
            cmpResult = _mm256_cmp_ps(error, _mm256_set1_ps(kThreshold), _CMP_GT_OQ);
            mask = _mm256_movemask_ps(cmpResult);
        }
        _mm256_storeu_ps(output+i, _mm256_mul_ps(target, guess));
    }
    // 如果用掩码更新，可以使用guess = _mm256_blendv_ps(guess, new_guess, mask);
    for (int i = aligned_n; i < N; i++) {
        float x = values[i];
        float guess = initialGuess;
        float error = fabs(guess * guess * x - 1.f);

        while (error > kThreshold) {
            guess = (3.f * guess - x * guess * guess * guess) * 0.5f;
            error = fabs(guess * guess * x - 1.f);
        }

        output[i] = x * guess;
    }
}