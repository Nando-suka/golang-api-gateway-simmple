package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Secret key untuk signing JWT (simpan di environment variable di production)
var jwtSecret = []byte("rahasia_super_secret_ubah_di_production")
	
// Claims struct – bisa ditambah field sesuai kebutuhan
type CustomClaims struct {
	UserID string `json:"user_id"`
	Role   string `json:"role"` // misal "admin", "user"
	jwt.RegisteredClaims
}

// GenerateJWT (untuk testing, membuat token valid)
func GenerateJWT(userID, role string) (string, error) {
	claims := CustomClaims{
		UserID: userID,
		Role:   role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "api-gateway",
			Subject:   userID,
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(jwtSecret)
}

// VerifyJWT memverifikasi token dan mengembalikan claims
func VerifyJWT(tokenString string) (*CustomClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Pastikan algoritma signing sesuai
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtSecret, nil
	})
	if err != nil {
		return nil, err
	}
	if claims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		return claims, nil
	}
	return nil, fmt.Errorf("invalid token")
}

// JWTMiddleware adalah middleware untuk autentikasi JWT
func JWTMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Ambil header Authorization
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Missing Authorization header", http.StatusUnauthorized)
			return
		}
		// Format: "Bearer <token>"
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			http.Error(w, "Invalid Authorization header format. Use 'Bearer <token>'", http.StatusUnauthorized)
			return
		}
		tokenString := parts[1]

		// Verifikasi token
		claims, err := VerifyJWT(tokenString)
		if err != nil {
			log.Printf("[JWT] Verification failed: %v", err)
			http.Error(w, "Invalid or expired token", http.StatusUnauthorized)
			return
		}
		// Simpan claims ke context request (opsional)
		ctx := context.WithValue(r.Context(), "user_id", claims.UserID)
		ctx = context.WithValue(ctx, "role", claims.Role)
		r = r.WithContext(ctx)

		log.Printf("[JWT] Authenticated user_id=%s role=%s", claims.UserID, claims.Role)
		next(w, r)
	}
}
