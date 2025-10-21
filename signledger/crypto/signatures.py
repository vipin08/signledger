"""Digital signature implementation for SignLedger."""

import base64
import json
from typing import Tuple, Optional, Union, Dict, Any, List
from abc import ABC, abstractmethod
import logging

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa, ec, ed25519, padding, utils
    from cryptography.hazmat.backends import default_backend
    from cryptography.exceptions import InvalidSignature
    HAS_CRYPTOGRAPHY = True
except ImportError:
    HAS_CRYPTOGRAPHY = False

logger = logging.getLogger(__name__)


class SignatureError(Exception):
    """Signature-related errors."""
    pass


class Signer(ABC):
    """Abstract base class for signers."""
    
    @abstractmethod
    def sign(self, data: bytes) -> str:
        """Sign data and return base64-encoded signature."""
        pass
    
    @abstractmethod
    def verify(self, data: bytes, signature: str) -> bool:
        """Verify signature for data."""
        pass
    
    @abstractmethod
    def get_public_key_pem(self) -> str:
        """Get public key in PEM format."""
        pass


class RSASigner(Signer):
    """RSA signature implementation."""
    
    def __init__(
        self,
        private_key: Optional[Union[str, bytes]] = None,
        public_key: Optional[Union[str, bytes]] = None,
        key_size: int = 2048,
    ):
        if not HAS_CRYPTOGRAPHY:
            raise ImportError("cryptography is required for signatures. Install with: pip install cryptography")
        
        if private_key:
            if isinstance(private_key, str):
                private_key = private_key.encode('utf-8')
            self._private_key = serialization.load_pem_private_key(
                private_key,
                password=None,
                backend=default_backend()
            )
            self._public_key = self._private_key.public_key()
        elif public_key:
            if isinstance(public_key, str):
                public_key = public_key.encode('utf-8')
            self._public_key = serialization.load_pem_public_key(
                public_key,
                backend=default_backend()
            )
            self._private_key = None
        else:
            # Generate new key pair
            self._private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=key_size,
                backend=default_backend()
            )
            self._public_key = self._private_key.public_key()
    
    def sign(self, data: bytes) -> str:
        """Sign data using RSA private key."""
        if not self._private_key:
            raise SignatureError("Private key required for signing")
        
        signature = self._private_key.sign(
            data,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        
        return base64.b64encode(signature).decode('utf-8')
    
    def verify(self, data: bytes, signature: str) -> bool:
        """Verify RSA signature."""
        try:
            signature_bytes = base64.b64decode(signature)
            
            self._public_key.verify(
                signature_bytes,
                data,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            
            return True
            
        except (InvalidSignature, Exception):
            return False
    
    def get_public_key_pem(self) -> str:
        """Get public key in PEM format."""
        pem = self._public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return pem.decode('utf-8')
    
    def get_private_key_pem(self) -> str:
        """Get private key in PEM format."""
        if not self._private_key:
            raise SignatureError("No private key available")
        
        pem = self._private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return pem.decode('utf-8')


class ECDSASigner(Signer):
    """ECDSA signature implementation."""
    
    def __init__(
        self,
        private_key: Optional[Union[str, bytes]] = None,
        public_key: Optional[Union[str, bytes]] = None,
        curve: str = "secp256r1",
    ):
        if not HAS_CRYPTOGRAPHY:
            raise ImportError("cryptography is required for signatures. Install with: pip install cryptography")
        
        # Map curve names to cryptography curves
        curve_map = {
            "secp256r1": ec.SECP256R1(),
            "secp384r1": ec.SECP384R1(),
            "secp521r1": ec.SECP521R1(),
            "secp256k1": ec.SECP256K1(),
        }
        
        if curve not in curve_map:
            raise ValueError(f"Unsupported curve: {curve}")
        
        self._curve = curve_map[curve]
        
        if private_key:
            if isinstance(private_key, str):
                private_key = private_key.encode('utf-8')
            self._private_key = serialization.load_pem_private_key(
                private_key,
                password=None,
                backend=default_backend()
            )
            self._public_key = self._private_key.public_key()
        elif public_key:
            if isinstance(public_key, str):
                public_key = public_key.encode('utf-8')
            self._public_key = serialization.load_pem_public_key(
                public_key,
                backend=default_backend()
            )
            self._private_key = None
        else:
            # Generate new key pair
            self._private_key = ec.generate_private_key(
                self._curve,
                backend=default_backend()
            )
            self._public_key = self._private_key.public_key()
    
    def sign(self, data: bytes) -> str:
        """Sign data using ECDSA private key."""
        if not self._private_key:
            raise SignatureError("Private key required for signing")
        
        signature = self._private_key.sign(
            data,
            ec.ECDSA(hashes.SHA256())
        )
        
        return base64.b64encode(signature).decode('utf-8')
    
    def verify(self, data: bytes, signature: str) -> bool:
        """Verify ECDSA signature."""
        try:
            signature_bytes = base64.b64decode(signature)
            
            self._public_key.verify(
                signature_bytes,
                data,
                ec.ECDSA(hashes.SHA256())
            )
            
            return True
            
        except (InvalidSignature, Exception):
            return False
    
    def get_public_key_pem(self) -> str:
        """Get public key in PEM format."""
        pem = self._public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return pem.decode('utf-8')
    
    def get_private_key_pem(self) -> str:
        """Get private key in PEM format."""
        if not self._private_key:
            raise SignatureError("No private key available")
        
        pem = self._private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return pem.decode('utf-8')


class Ed25519Signer(Signer):
    """Ed25519 signature implementation."""
    
    def __init__(
        self,
        private_key: Optional[Union[str, bytes]] = None,
        public_key: Optional[Union[str, bytes]] = None,
    ):
        if not HAS_CRYPTOGRAPHY:
            raise ImportError("cryptography is required for signatures. Install with: pip install cryptography")
        
        if private_key:
            if isinstance(private_key, str):
                private_key = private_key.encode('utf-8')
            self._private_key = serialization.load_pem_private_key(
                private_key,
                password=None,
                backend=default_backend()
            )
            self._public_key = self._private_key.public_key()
        elif public_key:
            if isinstance(public_key, str):
                public_key = public_key.encode('utf-8')
            self._public_key = serialization.load_pem_public_key(
                public_key,
                backend=default_backend()
            )
            self._private_key = None
        else:
            # Generate new key pair
            self._private_key = ed25519.Ed25519PrivateKey.generate()
            self._public_key = self._private_key.public_key()
    
    def sign(self, data: bytes) -> str:
        """Sign data using Ed25519 private key."""
        if not self._private_key:
            raise SignatureError("Private key required for signing")
        
        signature = self._private_key.sign(data)
        
        return base64.b64encode(signature).decode('utf-8')
    
    def verify(self, data: bytes, signature: str) -> bool:
        """Verify Ed25519 signature."""
        try:
            signature_bytes = base64.b64decode(signature)
            
            self._public_key.verify(signature_bytes, data)
            
            return True
            
        except (InvalidSignature, Exception):
            return False
    
    def get_public_key_pem(self) -> str:
        """Get public key in PEM format."""
        pem = self._public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return pem.decode('utf-8')
    
    def get_private_key_pem(self) -> str:
        """Get private key in PEM format."""
        if not self._private_key:
            raise SignatureError("No private key available")
        
        pem = self._private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return pem.decode('utf-8')


class MultiSigner:
    """Multi-signature support."""
    
    def __init__(self, required_signatures: int = 1):
        self.required_signatures = required_signatures
        self._signers: Dict[str, Signer] = {}
    
    def add_signer(self, key_id: str, signer: Signer) -> None:
        """Add a signer."""
        self._signers[key_id] = signer
    
    def remove_signer(self, key_id: str) -> bool:
        """Remove a signer."""
        if key_id in self._signers:
            del self._signers[key_id]
            return True
        return False
    
    def sign(self, data: bytes, key_ids: Optional[List[str]] = None) -> Dict[str, str]:
        """Sign data with multiple signers."""
        if key_ids is None:
            key_ids = list(self._signers.keys())
        
        signatures = {}
        
        for key_id in key_ids:
            if key_id in self._signers:
                try:
                    signature = self._signers[key_id].sign(data)
                    signatures[key_id] = signature
                except Exception as e:
                    logger.error(f"Failed to sign with key {key_id}: {e}")
        
        if len(signatures) < self.required_signatures:
            raise SignatureError(
                f"Insufficient signatures: {len(signatures)} < {self.required_signatures}"
            )
        
        return signatures
    
    def verify(self, data: bytes, signatures: Dict[str, str]) -> bool:
        """Verify multi-signatures."""
        valid_count = 0
        
        for key_id, signature in signatures.items():
            if key_id in self._signers:
                try:
                    if self._signers[key_id].verify(data, signature):
                        valid_count += 1
                except Exception as e:
                    logger.error(f"Failed to verify signature for key {key_id}: {e}")
        
        return valid_count >= self.required_signatures
    
    def get_public_keys(self) -> Dict[str, str]:
        """Get all public keys."""
        return {
            key_id: signer.get_public_key_pem()
            for key_id, signer in self._signers.items()
        }


def create_signer(algorithm: str = "RSA", **kwargs) -> Signer:
    """Factory function to create a signer."""
    algorithm = algorithm.upper()
    
    if algorithm == "RSA":
        return RSASigner(**kwargs)
    elif algorithm == "ECDSA":
        return ECDSASigner(**kwargs)
    elif algorithm == "ED25519":
        return Ed25519Signer(**kwargs)
    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}")